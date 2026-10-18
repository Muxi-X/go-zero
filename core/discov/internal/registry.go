package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/contextx"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/threading"
	rpctypes "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	registry = Registry{
		clusters: make(map[string]*cluster),
	}
	connManager = syncx.NewResourceManager()
)

// A Registry is a registry that manages the etcd client connections.
type Registry struct {
	clusters map[string]*cluster
	lock     sync.Mutex
}

// GetRegistry returns a global Registry.
func GetRegistry() *Registry {
	return &registry
}

// GetConn returns an etcd client connection associated with given endpoints.
func (r *Registry) GetConn(endpoints []string) (EtcdClient, error) {
	c, _ := r.getCluster(endpoints)
	return c.getClient()
}

// InvalidateConn removes the cached etcd client associated with given
// endpoints, but only when it is still cli. Conditional removal avoids closing
// a client that another watcher or publisher has already recreated.
func (r *Registry) InvalidateConn(endpoints []string, cli EtcdClient) error {
	return connManager.RemoveResourceIf(getClusterKey(endpoints), cli)
}

// Monitor monitors the key on given etcd endpoints, notify with the given UpdateListener.
func (r *Registry) Monitor(endpoints []string, key string, l UpdateListener) error {
	c, _ := r.getCluster(endpoints)
	return c.monitor(key, l)
}

func (r *Registry) getCluster(endpoints []string) (c *cluster, exists bool) {
	clusterKey := getClusterKey(endpoints)
	r.lock.Lock()
	defer r.lock.Unlock()
	c, exists = r.clusters[clusterKey]
	if !exists {
		c = newCluster(endpoints)
		r.clusters[clusterKey] = c
	}

	return
}

type cluster struct {
	endpoints  []string
	key        string
	values     map[string]map[string]string
	listeners  map[string][]UpdateListener
	watchGroup *threading.RoutineGroup
	// done is closed when the current watcher generation is replaced, see
	// reload. Watchers take their generation's channel as an argument instead
	// of reading this field, so it is only accessed under lock.
	done chan lang.PlaceholderType
	// started holds the keys that already have a watcher in the current
	// generation, so a concurrent reload and monitor do not start two watchers
	// for the same key.
	started map[string]struct{}
	lock    sync.Mutex
	// reloadLock serializes generations. Two concurrent reloads would
	// otherwise close the same channel and start duplicate watchers.
	reloadLock sync.Mutex
}

func newCluster(endpoints []string) *cluster {
	return &cluster{
		endpoints:  endpoints,
		key:        getClusterKey(endpoints),
		values:     make(map[string]map[string]string),
		listeners:  make(map[string][]UpdateListener),
		watchGroup: threading.NewRoutineGroup(),
		done:       make(chan lang.PlaceholderType),
		started:    make(map[string]struct{}),
	}
}

func (c *cluster) context(cli EtcdClient) context.Context {
	return contextx.ValueOnlyFrom(cli.Ctx())
}

func (c *cluster) getClient() (EtcdClient, error) {
	val, err := connManager.GetResource(c.key, func() (io.Closer, error) {
		return c.newClient()
	})
	if err != nil {
		return nil, err
	}

	return val.(EtcdClient), nil
}

func (c *cluster) handleChanges(key string, kvs []KV) {
	var add []KV
	var remove []KV
	c.lock.Lock()
	listeners := append([]UpdateListener(nil), c.listeners[key]...)
	vals, ok := c.values[key]
	if !ok {
		add = kvs
		vals = make(map[string]string)
		for _, kv := range kvs {
			vals[kv.Key] = kv.Val
		}
		c.values[key] = vals
	} else {
		m := make(map[string]string)
		for _, kv := range kvs {
			m[kv.Key] = kv.Val
		}
		for k, v := range vals {
			if val, ok := m[k]; !ok || v != val {
				remove = append(remove, KV{
					Key: k,
					Val: v,
				})
			}
		}
		for k, v := range m {
			if val, ok := vals[k]; !ok || v != val {
				add = append(add, KV{
					Key: k,
					Val: v,
				})
			}
		}
		c.values[key] = m
	}
	c.lock.Unlock()

	for _, kv := range add {
		for _, l := range listeners {
			l.OnAdd(kv)
		}
	}
	for _, kv := range remove {
		for _, l := range listeners {
			l.OnDelete(kv)
		}
	}
}

func (c *cluster) handleWatchEvents(key string, events []*clientv3.Event) {
	c.lock.Lock()
	listeners := append([]UpdateListener(nil), c.listeners[key]...)
	c.lock.Unlock()

	for _, ev := range events {
		switch ev.Type {
		case clientv3.EventTypePut:
			c.lock.Lock()
			if vals, ok := c.values[key]; ok {
				vals[string(ev.Kv.Key)] = string(ev.Kv.Value)
			} else {
				c.values[key] = map[string]string{string(ev.Kv.Key): string(ev.Kv.Value)}
			}
			c.lock.Unlock()
			for _, l := range listeners {
				l.OnAdd(KV{
					Key: string(ev.Kv.Key),
					Val: string(ev.Kv.Value),
				})
			}
		case clientv3.EventTypeDelete:
			c.lock.Lock()
			if vals, ok := c.values[key]; ok {
				delete(vals, string(ev.Kv.Key))
			}
			c.lock.Unlock()
			for _, l := range listeners {
				l.OnDelete(KV{
					Key: string(ev.Kv.Key),
					Val: string(ev.Kv.Value),
				})
			}
		default:
			logx.Errorf("Unknown event type: %v", ev.Type)
		}
	}
}

func (c *cluster) load(done chan lang.PlaceholderType, cli EtcdClient, key string) (int64, error) {
	var resp *clientv3.GetResponse
	for {
		select {
		case <-done:
			return 0, nil
		default:
		}

		// [Muxi Patch]
		// A client with a canceled context has been closed (e.g. by another
		// watcher's RemoveResourceIf, or the publisher's InvalidateConn). Get on
		// it can never succeed, and this loop has no exit other than the
		// watcher generation being replaced, which is not triggered by token
		// expiry. Without this check load() would spin forever on the dead
		// client, leaving the key's watcher stuck and the service on stale
		// addresses. Return instead so watch() can recreate the client.
		if cli.Ctx().Err() != nil {
			return 0, nil
		}

		var err error
		ctx, cancel := context.WithTimeout(c.context(cli), RequestTimeout)
		resp, err = cli.Get(ctx, makeKeyPrefix(key), clientv3.WithPrefix())
		cancel()
		if err == nil {
			break
		}
		// A retry can never succeed for errors like a missing read permission,
		// so report them instead of retrying forever. monitor() surfaces them
		// to the subscriber, watch() keeps retrying with a cooldown.
		if isTerminalGetError(err) {
			return 0, err
		}

		logx.Error(err)
		if !c.wait(done) {
			return 0, nil
		}
	}

	var kvs []KV
	for _, ev := range resp.Kvs {
		kvs = append(kvs, KV{
			Key: string(ev.Key),
			Val: string(ev.Value),
		})
	}

	c.handleChanges(key, kvs)

	return resp.Header.Revision, nil
}

// wait cools down before the next attempt, and reports whether the watcher
// generation is still alive.
func (c *cluster) wait(done chan lang.PlaceholderType) bool {
	timer := time.NewTimer(coolDownInterval)
	select {
	case <-timer.C:
		return true
	case <-done:
		timer.Stop()
		return false
	}
}

// recreateClient replaces a failed client with a new one, cooling down between
// attempts. It reports whether the watcher generation is still alive.
func (c *cluster) recreateClient(done chan lang.PlaceholderType) (EtcdClient, bool) {
	for {
		if !c.wait(done) {
			return nil, false
		}

		cli, err := c.getClient()
		if err != nil {
			logx.Errorf("recreate etcd client: %v", err)
			continue
		}
		// The freshly created client may have been closed by another failing
		// source racing with this recreation. Remove it by identity: the next
		// round has to invalidate this very client, otherwise it would stay in
		// the cache.
		if cli.Ctx().Err() != nil {
			logx.Errorf("etcd client invalidated during recreation, retry")
			if rerr := connManager.RemoveResourceIf(c.key, cli); rerr != nil {
				logx.Errorf("remove etcd client resource: %v", rerr)
			}
			continue
		}

		return cli, true
	}
}

// isTerminalGetError reports whether a failed Get can never succeed by
// retrying, e.g. when the credentials or the permissions are wrong. Transient
// failures, such as an unavailable cluster, are retried.
func isTerminalGetError(err error) bool {
	switch errorCode(err) {
	case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied:
		return true
	default:
		return false
	}
}

func errorCode(err error) codes.Code {
	var coder interface{ Code() codes.Code }
	if errors.As(err, &coder) {
		return coder.Code()
	}

	return status.Code(err)
}

func (c *cluster) monitor(key string, l UpdateListener) error {
	func() {
		c.lock.Lock()
		defer c.lock.Unlock()

		c.listeners[key] = append(c.listeners[key], l)
		// Replay the current values under the same lock that registers the
		// listener: an event delivered between a snapshot taken here and a
		// replay done afterwards could otherwise be overwritten by the older
		// snapshot. Listeners must not call back into the registry (e.g.
		// NewSubscriber) from OnAdd, or this deadlocks.
		for k, v := range c.values[key] {
			l.OnAdd(KV{Key: k, Val: v})
		}
	}()

	cli, err := c.getClient()
	if err != nil {
		c.removeListener(key, l)
		return err
	}

	// load() notifies the listener about the current values, and startWatch()
	// only accepts the generation that load() ran against. When a reload wins
	// the race, load() returns at once and the snapshot is taken again in the
	// new generation, so the key never misses its watcher or its values.
	for {
		done := c.currentDone()
		rev, err := c.load(done, cli, key)
		if err != nil {
			c.removeListener(key, l)
			// Leave a fresh client behind for the next attempt: an
			// authentication error means the cached one is unusable, and a
			// retry with it would fail the same way.
			if shouldInvalidateConn(cli, err) {
				if rerr := connManager.RemoveResourceIf(c.key, cli); rerr != nil {
					logx.Errorf("remove etcd client resource: %v", rerr)
				}
			}
			return err
		}

		if c.startWatch(done, cli, key, rev) {
			return nil
		}
	}
}

// removeListener drops l from the listeners of key, so a failed monitor leaves
// no orphan listener behind and a retried NewSubscriber receives no duplicate
// events.
func (c *cluster) removeListener(key string, l UpdateListener) {
	c.lock.Lock()
	defer c.lock.Unlock()

	listeners := c.listeners[key]
	for i, item := range listeners {
		if item == l {
			listeners = append(listeners[:i], listeners[i+1:]...)
			if len(listeners) == 0 {
				delete(c.listeners, key)
			} else {
				c.listeners[key] = listeners
			}
			return
		}
	}
}

func (c *cluster) currentDone() chan lang.PlaceholderType {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.done
}

// startWatch starts a watcher for key in the given generation. It reports
// whether that generation was still the current one when the watcher was
// registered. When it was, the key ends up with exactly one watcher, because
// reload marks every key it covers before it starts them.
func (c *cluster) startWatch(done chan lang.PlaceholderType, cli EtcdClient, key string, rev int64) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.done != done {
		return false
	}

	if _, ok := c.started[key]; ok {
		return true
	}

	c.started[key] = struct{}{}
	c.watchGroup.Run(func() {
		c.watch(done, cli, key, rev)
	})

	return true
}

func (c *cluster) newClient() (EtcdClient, error) {
	cli, err := NewClient(c.endpoints)
	if err != nil {
		return nil, err
	}

	go c.watchConnState(cli)

	return cli, nil
}

func (c *cluster) reload() {
	c.reloadLock.Lock()
	defer c.reloadLock.Unlock()

	// always use the currently cached client from the connection manager,
	// otherwise a stale (closed) client could be used to reload. Since
	// RemoveResourceIf atomically deletes before closing, the cache never
	// holds a closed client.
	cli, err := c.getClient()
	if err != nil {
		return
	}

	// Swap the generation under the lock, but wait for the previous one
	// outside of it: watchers take c.lock in handleChanges/handleWatchEvents,
	// so waiting with the lock held deadlocks with them.
	c.lock.Lock()
	oldDone := c.done
	oldGroup := c.watchGroup
	done := make(chan lang.PlaceholderType)
	group := threading.NewRoutineGroup()
	c.done = done
	c.watchGroup = group
	c.started = make(map[string]struct{}, len(c.listeners))
	var keys []string
	for k := range c.listeners {
		keys = append(keys, k)
		c.started[k] = struct{}{}
	}
	c.lock.Unlock()

	close(oldDone)
	oldGroup.Wait()

	for _, key := range keys {
		k := key
		group.Run(func() {
			client := cli
			for {
				rev, err := c.load(done, client, k)
				if err == nil {
					c.watch(done, client, k, rev)
					return
				}

				// Retrying the load keeps the snapshot gap-free: watching from
				// 0 after the error is cleared would skip every change made
				// while it persisted.
				logx.Error(err)
				// An authentication error means the cached client is unusable:
				// retrying the load with it would fail the same way, leaving
				// the key stale forever.
				if !shouldInvalidateConn(client, err) {
					if !c.wait(done) {
						return
					}
					continue
				}

				if rerr := connManager.RemoveResourceIf(c.key, client); rerr != nil {
					logx.Errorf("remove etcd client resource: %v", rerr)
				}
				newCli, ok := c.recreateClient(done)
				if !ok {
					return
				}
				client = newCli
			}
		})
	}
}

func (c *cluster) watch(done chan lang.PlaceholderType, cli EtcdClient, key string, rev int64) {
	for {
		err := c.watchStream(done, cli, key, rev)
		if err == nil {
			return
		}
		logx.Errorf("etcd monitor chan exited: %v", err)

		select {
		case <-done:
			return
		default:
		}

		// [Muxi Patch]
		// A long-lived watch fails with "invalid auth token" once the auth
		// token expires. Rebuilding the stream is not always enough: clientv3
		// re-authenticates when it creates a stream, but getToken sends the
		// current (expired) token along with the Authenticate call, and
		// servers without etcd-io/etcd#12165 reject it. Replacing the client
		// resets its token bundle, which forces a full re-authentication.
		//
		// Other failures (e.g. a compacted revision) are not the client's
		// fault, and the client is shared by every watcher and publisher on
		// the same endpoints, so closing it would interrupt healthy streams
		// for no reason; reloading the revision is enough to recover from
		// them.
		//
		// Removal is conditional on cli: a concurrent watcher may have already
		// replaced the cached client, and closing that fresh client would
		// disrupt it for no reason. Single-flight guarantees only one client
		// is recreated even when multiple watchers fail together.
		if shouldInvalidateConn(cli, err) {
			if err := connManager.RemoveResourceIf(c.key, cli); err != nil {
				logx.Errorf("remove etcd client resource: %v", err)
			}

			newCli, ok := c.recreateClient(done)
			if !ok {
				return
			}
			cli = newCli
		} else {
			// Retry with a cooldown to avoid a tight loop.
			if !c.wait(done) {
				return
			}
		}

		newRev, err := c.load(done, cli, key)
		if err != nil {
			// Keep the last good revision. Watching from 0 after the error is
			// cleared would silently skip every change made during the outage.
			logx.Error(err)
			if !c.wait(done) {
				return
			}
			continue
		}
		rev = newRev
	}
}

// shouldInvalidateConn reports whether a failed watch means that the client
// itself is unusable and has to be replaced instead of reused. That is the
// case when the client has already been closed, or when the server no longer
// accepts its credentials, which a stream rebuilt on the same client cannot
// fix (see the [Muxi Patch] comment in watch()).
func shouldInvalidateConn(cli EtcdClient, err error) bool {
	if cli.Ctx().Err() != nil {
		return true
	}

	return isAuthError(err)
}

// isAuthError reports whether err means the client's credentials are no longer
// accepted by the server. clientv3 refreshes the token for the same set of
// errors in Client.shouldRefreshToken.
func isAuthError(err error) bool {
	// Watch errors are normalized by clientv3 into rpctypes values, while a
	// failed unary call returns the raw gRPC status error.
	if errorCode(err) == codes.Unauthenticated {
		return true
	}

	err = rpctypes.Error(err)
	return errors.Is(err, rpctypes.ErrInvalidAuthToken) ||
		errors.Is(err, rpctypes.ErrAuthOldRevision) ||
		errors.Is(err, rpctypes.ErrUserEmpty)
}

func (c *cluster) watchStream(done chan lang.PlaceholderType, cli EtcdClient, key string, rev int64) error {
	// Cancel the watch when this stream is no longer consumed. Otherwise
	// clientv3 keeps the substream attached to the shared client: its one-slot
	// response buffer fills, then the client's dispatch loop blocks on it and
	// every other watch on the same endpoints stops receiving events.
	ctx, cancel := context.WithCancel(c.context(cli))
	defer cancel()

	var rch clientv3.WatchChan
	if rev != 0 {
		rch = cli.Watch(clientv3.WithRequireLeader(ctx), makeKeyPrefix(key), clientv3.WithPrefix(),
			clientv3.WithRev(rev+1))
	} else {
		rch = cli.Watch(clientv3.WithRequireLeader(ctx), makeKeyPrefix(key), clientv3.WithPrefix())
	}

	for {
		select {
		case wresp, ok := <-rch:
			if !ok {
				return errors.New("etcd monitor chan has been closed")
			}
			if wresp.Canceled {
				return fmt.Errorf("etcd monitor chan has been canceled, error: %w", wresp.Err())
			}
			if err := wresp.Err(); err != nil {
				return fmt.Errorf("etcd monitor chan error: %w", err)
			}

			c.handleWatchEvents(key, wresp.Events)
		case <-done:
			return nil
		}
	}
}

func (c *cluster) watchConnState(cli EtcdClient) {
	watcher := newStateWatcher()
	watcher.addListener(func() {
		go c.reload()
	})
	// pass cli.Ctx() so the goroutine exits once the client is closed,
	// otherwise it leaks forever on a Shutdown connection.
	watcher.watch(cli.Ctx(), cli.ActiveConnection())
}

// DialClient dials an etcd cluster with given endpoints.
func DialClient(endpoints []string) (EtcdClient, error) {
	cfg := clientv3.Config{
		Endpoints:            endpoints,
		AutoSyncInterval:     autoSyncInterval,
		DialTimeout:          DialTimeout,
		DialKeepAliveTime:    dialKeepAliveTime,
		DialKeepAliveTimeout: DialTimeout,
		RejectOldCluster:     true,
		PermitWithoutStream:  true,
	}
	if account, ok := GetAccount(endpoints); ok {
		cfg.Username = account.User
		cfg.Password = account.Pass
	}
	if tlsCfg, ok := GetTLS(endpoints); ok {
		cfg.TLS = tlsCfg
	}

	return clientv3.New(cfg)
}

func getClusterKey(endpoints []string) string {
	sort.Strings(endpoints)
	return strings.Join(endpoints, endpointsSeparator)
}

func makeKeyPrefix(key string) string {
	return fmt.Sprintf("%s%c", key, Delimiter)
}
