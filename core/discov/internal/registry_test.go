package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/contextx"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stringx"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	rpctypes "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var mockLock sync.Mutex

func init() {
	logx.Disable()
}

func setMockClient(cli EtcdClient) func() {
	mockLock.Lock()
	NewClient = func([]string) (EtcdClient, error) {
		return cli, nil
	}
	return func() {
		NewClient = DialClient
		mockLock.Unlock()
	}
}

func TestGetCluster(t *testing.T) {
	AddAccount([]string{"first"}, "foo", "bar")
	c1, _ := GetRegistry().getCluster([]string{"first"})
	c2, _ := GetRegistry().getCluster([]string{"second"})
	c3, _ := GetRegistry().getCluster([]string{"first"})
	assert.Equal(t, c1, c3)
	assert.NotEqual(t, c1, c2)
}

func TestGetClusterKey(t *testing.T) {
	assert.Equal(t, getClusterKey([]string{"localhost:1234", "remotehost:5678"}),
		getClusterKey([]string{"remotehost:5678", "localhost:1234"}))
}

func TestRegistry_InvalidateConn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	endpoints := []string{"invalidate-" + stringx.Rand()}
	key := getClusterKey(endpoints)
	cli := NewMockEtcdClient(ctrl)
	cli.EXPECT().Close().Return(nil)

	connManager.Inject(key, cli)

	assert.NoError(t, GetRegistry().InvalidateConn(endpoints, cli))

	// The cached entry was removed, so GetResource creates a fresh one.
	created := NewMockEtcdClient(ctrl)
	val, err := connManager.GetResource(key, func() (io.Closer, error) {
		return created, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, created, val)
}

func TestRegistry_InvalidateConnStaleClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	endpoints := []string{"invalidate-stale-" + stringx.Rand()}
	key := getClusterKey(endpoints)
	cached := NewMockEtcdClient(ctrl)
	stale := NewMockEtcdClient(ctrl)
	// The cached client belongs to someone else and must not be closed.
	cached.EXPECT().Close().Times(0)

	connManager.Inject(key, cached)

	assert.NoError(t, GetRegistry().InvalidateConn(endpoints, stale))

	// The cached client is still in place.
	val, err := connManager.GetResource(key, func() (io.Closer, error) {
		return stale, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, cached, val)
}

func TestCluster_HandleChanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	l := NewMockUpdateListener(ctrl)
	l.EXPECT().OnAdd(KV{
		Key: "first",
		Val: "1",
	})
	l.EXPECT().OnAdd(KV{
		Key: "second",
		Val: "2",
	})
	l.EXPECT().OnDelete(KV{
		Key: "first",
		Val: "1",
	})
	l.EXPECT().OnDelete(KV{
		Key: "second",
		Val: "2",
	})
	l.EXPECT().OnAdd(KV{
		Key: "third",
		Val: "3",
	})
	l.EXPECT().OnAdd(KV{
		Key: "fourth",
		Val: "4",
	})
	c := newCluster([]string{"any"})
	c.listeners["any"] = []UpdateListener{l}
	c.handleChanges("any", []KV{
		{
			Key: "first",
			Val: "1",
		},
		{
			Key: "second",
			Val: "2",
		},
	})
	assert.EqualValues(t, map[string]string{
		"first":  "1",
		"second": "2",
	}, c.values["any"])
	c.handleChanges("any", []KV{
		{
			Key: "third",
			Val: "3",
		},
		{
			Key: "fourth",
			Val: "4",
		},
	})
	assert.EqualValues(t, map[string]string{
		"third":  "3",
		"fourth": "4",
	}, c.values["any"])
}

func TestCluster_Load(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cli := NewMockEtcdClient(ctrl)
	restore := setMockClient(cli)
	defer restore()
	cli.EXPECT().Get(gomock.Any(), "any/", gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{},
		Kvs: []*mvccpb.KeyValue{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		},
	}, nil)
	cli.EXPECT().Ctx().Return(context.Background()).AnyTimes()
	c := &cluster{
		values: make(map[string]map[string]string),
	}
	rev, err := c.load(nil, cli, "any")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rev)
}

func TestCluster_Watch(t *testing.T) {
	tests := []struct {
		name      string
		method    int
		eventType mvccpb.Event_EventType
	}{
		{
			name:      "add",
			eventType: clientv3.EventTypePut,
		},
		{
			name:      "delete",
			eventType: clientv3.EventTypeDelete,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			cli := NewMockEtcdClient(ctrl)
			restore := setMockClient(cli)
			defer restore()
			ch := make(chan clientv3.WatchResponse)
			cli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).Return(ch)
			cli.EXPECT().Ctx().Return(context.Background())
			handled := make(chan struct{}, 1)
			c := &cluster{
				listeners: make(map[string][]UpdateListener),
				values:    make(map[string]map[string]string),
			}
			listener := NewMockUpdateListener(ctrl)
			c.listeners["any"] = []UpdateListener{listener}
			listener.EXPECT().OnAdd(gomock.Any()).Do(func(kv KV) {
				assert.Equal(t, "hello", kv.Key)
				assert.Equal(t, "world", kv.Val)
				handled <- struct{}{}
			}).MaxTimes(1)
			listener.EXPECT().OnDelete(gomock.Any()).Do(func(_ interface{}) {
				handled <- struct{}{}
			}).MaxTimes(1)
			go c.watch(c.done, cli, "any", 0)
			ch <- clientv3.WatchResponse{
				Events: []*clientv3.Event{
					{
						Type: test.eventType,
						Kv: &mvccpb.KeyValue{
							Key:   []byte("hello"),
							Value: []byte("world"),
						},
					},
				},
			}
			select {
			case <-handled:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for the watch event")
			}
		})
	}
}

func TestClusterWatch_RespFailures(t *testing.T) {
	resps := []clientv3.WatchResponse{
		{
			Canceled: true,
		},
		{
			// cause resp.Err() != nil
			CompactRevision: 1,
		},
	}

	for _, resp := range resps {
		t.Run(stringx.Rand(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			cli := NewMockEtcdClient(ctrl)
			restore := setMockClient(cli)
			defer restore()
			ch := make(chan clientv3.WatchResponse)
			cli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).Return(ch).AnyTimes()
			cli.EXPECT().Ctx().Return(context.Background()).AnyTimes()
			c := new(cluster)
			c.done = make(chan lang.PlaceholderType)
			go func() {
				ch <- resp
				close(c.done)
			}()
			c.watch(c.done, cli, "any", 0)
		})
	}
}

func TestClusterWatch_CloseChan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cli := NewMockEtcdClient(ctrl)
	restore := setMockClient(cli)
	defer restore()
	ch := make(chan clientv3.WatchResponse)
	cli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).Return(ch).AnyTimes()
	cli.EXPECT().Ctx().Return(context.Background()).AnyTimes()
	c := new(cluster)
	c.done = make(chan lang.PlaceholderType)
	go func() {
		close(ch)
		close(c.done)
	}()
	c.watch(c.done, cli, "any", 0)
}

// TestClusterWatch_CompactedRevisionKeepsClient covers the shared-client
// protection: a compacted revision is a watch-specific failure, not a broken
// client. The watcher must reload the revision instead of invalidating the
// cached client, which is shared by every watcher and publisher on the same
// endpoints.
func TestClusterWatch_CompactedRevisionKeepsClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := createMockConn(t)
	defer conn.Close()

	cli := NewMockEtcdClient(ctrl)
	cli.EXPECT().Ctx().Return(ctx).AnyTimes()
	cli.EXPECT().ActiveConnection().Return(conn).AnyTimes()
	// The client is healthy, so it must not be closed.
	cli.EXPECT().Close().Times(0)

	watchCh := make(chan clientv3.WatchResponse)
	cli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).Return(watchCh).AnyTimes()

	reloaded := make(chan struct{})
	cli.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			select {
			case <-reloaded:
			default:
				close(reloaded)
			}
			return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{}}, nil
		}).AnyTimes()

	restore := setMockClients(cli)
	defer restore()

	c := new(cluster)
	c.endpoints = []string{"compacted-" + stringx.Rand()}
	c.key = getClusterKey(c.endpoints)
	c.done = make(chan lang.PlaceholderType)
	c.listeners = make(map[string][]UpdateListener)
	c.values = make(map[string]map[string]string)

	// Cache the client the way monitor() does, so that a wrongful
	// invalidation would remove and close it.
	if _, err := c.getClient(); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		c.watch(c.done, cli, "any", 10)
		close(done)
	}()

	watchCh <- clientv3.WatchResponse{Canceled: true, CompactRevision: 1}

	select {
	case <-reloaded:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not reload the revision after a compacted response")
	}

	close(c.done)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not exit")
	}
}

// TestShouldInvalidateConn covers which watch failures replace the client.
// Only a closed client or an expired auth token requires a new client; other
// failures must keep the shared client alive.
func TestShouldInvalidateConn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	aliveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	alive := NewMockEtcdClient(ctrl)
	alive.EXPECT().Ctx().Return(aliveCtx).AnyTimes()

	deadCtx, cancelDead := context.WithCancel(context.Background())
	cancelDead()
	dead := NewMockEtcdClient(ctrl)
	dead.EXPECT().Ctx().Return(deadCtx).AnyTimes()

	tests := []struct {
		name string
		cli  EtcdClient
		err  error
		want bool
	}{
		{
			name: "expired auth token",
			cli:  alive,
			// The shape produced by clientv3's WatchResponse.Err() for a
			// stream that failed to authenticate.
			err: fmt.Errorf("etcd monitor chan has been canceled, error: %w",
				rpctypes.Error(status.Error(codes.Unauthenticated, "etcdserver: invalid auth token"))),
			want: true,
		},
		{
			name: "stale auth revision",
			cli:  alive,
			err: fmt.Errorf("etcd monitor chan has been canceled, error: %w",
				rpctypes.Error(status.Error(codes.InvalidArgument, "etcdserver: revision of auth store is old"))),
			want: true,
		},
		{
			name: "empty user",
			cli:  alive,
			err: fmt.Errorf("etcd monitor chan has been canceled, error: %w",
				rpctypes.Error(status.Error(codes.InvalidArgument, "etcdserver: user name is empty"))),
			want: true,
		},
		{
			name: "closed client",
			cli:  dead,
			err:  errors.New("etcd monitor chan has been closed"),
			want: true,
		},
		{
			name: "compacted revision",
			cli:  alive,
			err:  rpctypes.ErrCompacted,
			want: false,
		},
		{
			name: "future revision",
			cli:  alive,
			err:  rpctypes.ErrFutureRev,
			want: false,
		},
		{
			name: "permission denied",
			cli:  alive,
			err:  rpctypes.ErrPermissionDenied,
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, shouldInvalidateConn(test.cli, test.err))
		})
	}
}

// TestCluster_LoadOnClosedClientReturns covers the load() fix: when the client
// has been closed (its Ctx is canceled), load() must return promptly instead of
// spinning forever on Get. It must not issue a Get on the dead client at all;
// the caller (watch/monitor/reload) then recreates the client.
func TestCluster_LoadOnClosedClientReturns(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	deadCtx, cancel := context.WithCancel(context.Background())
	cancel()

	cli := NewMockEtcdClient(ctrl)
	cli.EXPECT().Ctx().Return(deadCtx).AnyTimes()
	// Get on a closed client can never succeed; it must not be attempted.
	cli.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	c := &cluster{
		done:   make(chan lang.PlaceholderType),
		values: make(map[string]map[string]string),
	}

	done := make(chan struct{})
	go func() {
		rev, err := c.load(c.done, cli, "any")
		assert.NoError(t, err)
		assert.Equal(t, int64(0), rev)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("load did not return on a closed client")
	}
}

func TestValueOnlyContext(t *testing.T) {
	ctx := contextx.ValueOnlyFrom(context.Background())
	ctx.Done()
	assert.Nil(t, ctx.Err())
}

func setMockClients(clis ...EtcdClient) func() {
	mockLock.Lock()
	index := 0
	NewClient = func([]string) (EtcdClient, error) {
		if index >= len(clis) {
			panic("unexpected etcd client creation")
		}

		cli := clis[index]
		index++
		return cli, nil
	}
	return func() {
		NewClient = DialClient
		mockLock.Unlock()
	}
}

func createMockConn(t *testing.T) *grpc.ClientConn {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	lis.Close()
	return conn
}

// TestClusterWatch_RecreatedClientInvalidated covers the Bug 1 fix: if a
// freshly recreated client is concurrently closed by another invalidation
// source (its Ctx is already canceled), watch() must not proceed to load() on
// the dead client; it redoes the invalidation+recreation cycle until a live
// client is obtained.
func TestClusterWatch_RecreatedClientInvalidated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	deadCtx, cancel := context.WithCancel(context.Background())
	cancel()

	deadCli := NewMockEtcdClient(ctrl)
	deadCli.EXPECT().Ctx().Return(deadCtx).AnyTimes()
	deadCli.EXPECT().Close().Return(nil).AnyTimes()

	aliveCli := NewMockEtcdClient(ctrl)
	aliveCli.EXPECT().Ctx().Return(context.Background()).AnyTimes()

	conn := createMockConn(t)
	defer conn.Close()
	deadCli.EXPECT().ActiveConnection().Return(conn).AnyTimes()
	aliveCli.EXPECT().ActiveConnection().Return(conn).AnyTimes()

	// Two watch failures drive the recreation loop; both are consumed by the
	// initial (dead) client's watchStream.
	watchCh := make(chan clientv3.WatchResponse, 2)
	watchCh <- clientv3.WatchResponse{Canceled: true}
	watchCh <- clientv3.WatchResponse{Canceled: true}
	deadCli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).Return(watchCh).AnyTimes()
	aliveCli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).Return(watchCh).AnyTimes()

	aliveGet := make(chan struct{})
	aliveCli.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			select {
			case <-aliveGet:
			default:
				close(aliveGet)
			}
			return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{}}, nil
		}).AnyTimes()

	restore := setMockClients(deadCli, aliveCli)
	defer restore()

	c := new(cluster)
	c.key = "recreated-invalidated-" + stringx.Rand()
	c.done = make(chan lang.PlaceholderType)
	c.listeners = make(map[string][]UpdateListener)
	c.values = make(map[string]map[string]string)

	done := make(chan struct{})
	go func() {
		c.watch(c.done, deadCli, "any", 0)
		close(done)
	}()

	// First failure -> recreate -> getClient returns deadCli (Ctx canceled)
	// -> the Bug 1 guard retries; second failure -> getClient returns aliveCli
	// -> load(aliveCli) succeeds.
	select {
	case <-aliveGet:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the alive client to be used")
	}

	close(c.done)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not exit")
	}
}

// TestClusterWatch_DeadRecreatedClientRemoved covers the guard's recovery
// path: when the recreated client has already been closed by another source,
// watch() switches to it and invalidates it by identity on the next round.
// Retrying with the previous client would never match the client cached under
// the cluster key, so the dead client would never be removed and every round
// would only spend a cooldown before failing again.
func TestClusterWatch_DeadRecreatedClientRemoved(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	cancelFirst()
	deadCtx, cancelDead := context.WithCancel(context.Background())
	cancelDead()
	aliveCtx, cancelAlive := context.WithCancel(context.Background())
	defer cancelAlive()

	conn := createMockConn(t)
	defer conn.Close()

	// The initial client fails the watch and gets invalidated.
	firstCli := NewMockEtcdClient(ctrl)
	firstCli.EXPECT().Ctx().Return(firstCtx).AnyTimes()
	firstFailures := make(chan clientv3.WatchResponse, 2)
	firstFailures <- clientv3.WatchResponse{Canceled: true}
	firstFailures <- clientv3.WatchResponse{Canceled: true}
	firstCli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).Return(firstFailures).AnyTimes()

	// The first recreation returns a client that has already been closed, so
	// it has to be removed by identity on the next round.
	deadCli := NewMockEtcdClient(ctrl)
	deadCli.EXPECT().Ctx().Return(deadCtx).AnyTimes()
	deadCli.EXPECT().ActiveConnection().Return(conn).AnyTimes()
	closedWatch := make(chan clientv3.WatchResponse)
	close(closedWatch)
	deadCli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).Return(closedWatch).AnyTimes()
	deadCli.EXPECT().Close().Return(nil).Times(1)

	// The second recreation is live and must be used.
	aliveCli := NewMockEtcdClient(ctrl)
	aliveCli.EXPECT().Ctx().Return(aliveCtx).AnyTimes()
	aliveCli.EXPECT().ActiveConnection().Return(conn).AnyTimes()
	aliveCli.EXPECT().Close().Return(nil).AnyTimes()
	aliveWatch := make(chan clientv3.WatchResponse)
	aliveCli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).Return(aliveWatch).AnyTimes()

	loaded := make(chan struct{})
	aliveCli.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			select {
			case <-loaded:
			default:
				close(loaded)
			}
			return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{}}, nil
		}).AnyTimes()

	restore := setMockClients(deadCli, aliveCli)
	defer restore()

	c := new(cluster)
	c.endpoints = []string{"dead-recreated-" + stringx.Rand()}
	c.key = getClusterKey(c.endpoints)
	c.done = make(chan lang.PlaceholderType)
	c.listeners = make(map[string][]UpdateListener)
	c.values = make(map[string]map[string]string)
	defer func() {
		_ = connManager.RemoveResourceIf(c.key, aliveCli)
	}()

	done := make(chan struct{})
	go func() {
		c.watch(c.done, firstCli, "any", 0)
		close(done)
	}()

	select {
	case <-loaded:
	case <-time.After(6 * time.Second):
		t.Fatal("watch did not recover from the closed recreated client")
	}

	close(c.done)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not exit")
	}
}

// TestClusterWatch_ConcurrentInvalidation covers the shared-client property:
// when several watchers fail on the same cached client together, only one of
// them removes and closes it, and all of them continue on the single recreated
// client.
func TestClusterWatch_ConcurrentInvalidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const watchers = 3

	deadCtx, cancelDead := context.WithCancel(context.Background())
	cancelDead()
	aliveCtx, cancelAlive := context.WithCancel(context.Background())
	defer cancelAlive()

	conn := createMockConn(t)
	defer conn.Close()

	deadCli := NewMockEtcdClient(ctrl)
	deadCli.EXPECT().Ctx().Return(deadCtx).AnyTimes()
	deadCli.EXPECT().ActiveConnection().Return(conn).AnyTimes()
	closedWatch := make(chan clientv3.WatchResponse)
	close(closedWatch)
	deadCli.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(closedWatch).AnyTimes()
	// Only the watcher that still sees the client cached removes it.
	deadCli.EXPECT().Close().Return(nil).Times(1)

	aliveCli := NewMockEtcdClient(ctrl)
	aliveCli.EXPECT().Ctx().Return(aliveCtx).AnyTimes()
	aliveCli.EXPECT().ActiveConnection().Return(conn).AnyTimes()
	aliveCli.EXPECT().Close().Return(nil).AnyTimes()
	aliveWatch := make(chan clientv3.WatchResponse)
	aliveCli.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(aliveWatch).AnyTimes()

	loaded := make(chan struct{}, watchers)
	aliveCli.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			loaded <- struct{}{}
			return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{}}, nil
		}).Times(watchers)

	restore := setMockClients(deadCli, aliveCli)
	defer restore()

	c := new(cluster)
	c.endpoints = []string{"concurrent-" + stringx.Rand()}
	c.key = getClusterKey(c.endpoints)
	c.done = make(chan lang.PlaceholderType)
	c.listeners = make(map[string][]UpdateListener)
	c.values = make(map[string]map[string]string)
	defer func() {
		_ = connManager.RemoveResourceIf(c.key, aliveCli)
	}()

	// Cache the client the way monitor() does.
	if _, err := c.getClient(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(watchers)
	for i := 0; i < watchers; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			c.watch(c.done, deadCli, key, 0)
		}()
	}

	for i := 0; i < watchers; i++ {
		select {
		case <-loaded:
		case <-time.After(6 * time.Second):
			t.Fatal("timed out waiting for the watchers to use the recreated client")
		}
	}

	close(c.done)
	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()

	select {
	case <-allDone:
	case <-time.After(2 * time.Second):
		t.Fatal("watchers did not exit")
	}
}

// TestCluster_ReloadDoesNotHoldLockWhileWaiting covers the reload deadlock:
// reload used to keep c.lock while waiting for the watchers, which take the
// same lock in handleChanges, so a watcher finishing its load blocked forever
// and froze the whole cluster.
func TestCluster_ReloadDoesNotHoldLockWhileWaiting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cli := NewMockEtcdClient(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli.EXPECT().Ctx().Return(ctx).AnyTimes()
	conn := createMockConn(t)
	defer conn.Close()
	cli.EXPECT().ActiveConnection().Return(conn).AnyTimes()
	cli.EXPECT().Close().Return(nil).AnyTimes()

	// Block the watcher's Get until reload has replaced the generation.
	getEntered := make(chan struct{})
	releaseGet := make(chan struct{})
	cli.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			select {
			case <-getEntered:
			default:
				close(getEntered)
			}
			<-releaseGet
			return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{}}, nil
		}).AnyTimes()
	// The watcher exits as soon as it sees the old generation closed.
	cli.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(make(chan clientv3.WatchResponse)).AnyTimes()

	restore := setMockClient(cli)
	defer restore()

	c := newCluster([]string{"reload-lock-" + stringx.Rand()})
	defer func() {
		_ = connManager.RemoveResourceIf(c.key, cli)
	}()

	done := c.currentDone()
	c.watchGroup.Run(func() {
		rev, _ := c.load(done, cli, "any")
		c.watch(done, cli, "any", rev)
	})

	select {
	case <-getEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not reach load")
	}

	reloaded := make(chan struct{})
	go func() {
		c.reload()
		close(reloaded)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reload did not replace the generation")
	}

	// The watcher can now reach handleChanges, which takes c.lock. reload must
	// not be holding it while waiting for the watcher to exit.
	close(releaseGet)

	select {
	case <-reloaded:
	case <-time.After(3 * time.Second):
		t.Fatal("reload deadlocked waiting for the watcher")
	}
}

// TestCluster_StartWatchOncePerGeneration covers the registration rule: a key
// has exactly one watcher per generation, and a generation that has already
// been replaced cannot be registered into.
func TestCluster_StartWatchOncePerGeneration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cli := NewMockEtcdClient(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli.EXPECT().Ctx().Return(ctx).AnyTimes()
	// A second watcher for the same key would call Watch twice.
	watchCalled := make(chan struct{}, 1)
	cli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).DoAndReturn(
		func(context.Context, string, ...clientv3.OpOption) clientv3.WatchChan {
			watchCalled <- struct{}{}
			return make(chan clientv3.WatchResponse)
		}).Times(1)

	c := newCluster([]string{"start-watch-" + stringx.Rand()})
	done := c.currentDone()
	defer close(done)

	if !c.startWatch(done, cli, "any", 0) {
		t.Fatal("first startWatch should start a watcher")
	}

	select {
	case <-watchCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not start")
	}

	if !c.startWatch(done, cli, "any", 0) {
		t.Fatal("second startWatch should report the generation as current")
	}

	c.lock.Lock()
	c.done = make(chan lang.PlaceholderType)
	c.started = make(map[string]struct{})
	c.lock.Unlock()

	if c.startWatch(done, cli, "any", 0) {
		t.Fatal("startWatch should reject a replaced generation")
	}
}

// TestCluster_ReloadKeepsMonitoredKeys covers that a reload re-watches every
// monitored key in the new generation.
func TestCluster_ReloadKeepsMonitoredKeys(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cli := NewMockEtcdClient(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli.EXPECT().Ctx().Return(ctx).AnyTimes()
	conn := createMockConn(t)
	defer conn.Close()
	cli.EXPECT().ActiveConnection().Return(conn).AnyTimes()
	cli.EXPECT().Close().Return(nil).AnyTimes()

	cli.EXPECT().Get(gomock.Any(), "any/", gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{},
	}, nil).AnyTimes()
	watched := make(chan struct{}, 4)
	cli.EXPECT().Watch(gomock.Any(), "any/", gomock.Any()).DoAndReturn(
		func(context.Context, string, ...clientv3.OpOption) clientv3.WatchChan {
			watched <- struct{}{}
			return make(chan clientv3.WatchResponse)
		}).AnyTimes()

	restore := setMockClient(cli)
	defer restore()

	c := newCluster([]string{"reload-monitored-" + stringx.Rand()})
	defer func() {
		_ = connManager.RemoveResourceIf(c.key, cli)
	}()

	listener := NewMockUpdateListener(ctrl)
	assert.NoError(t, c.monitor("any", listener))

	select {
	case <-watched:
	case <-time.After(2 * time.Second):
		t.Fatal("monitor did not start a watcher")
	}

	c.reload()

	select {
	case <-watched:
	case <-time.After(2 * time.Second):
		t.Fatal("reload did not re-watch the monitored key")
	}

	close(c.currentDone())
}

// TestCluster_MonitorReturnsTerminalError covers that a Get failure that can
// never succeed, e.g. a missing read permission, is reported to the subscriber
// instead of retrying forever and leaving NewSubscriber stuck.
func TestCluster_MonitorReturnsTerminalError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cli := NewMockEtcdClient(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli.EXPECT().Ctx().Return(ctx).AnyTimes()
	conn := createMockConn(t)
	defer conn.Close()
	cli.EXPECT().ActiveConnection().Return(conn).AnyTimes()
	cli.EXPECT().Close().Return(nil).AnyTimes()

	cli.EXPECT().Get(gomock.Any(), "any/", gomock.Any()).Return(nil,
		status.Error(codes.PermissionDenied, "etcdserver: permission denied")).Times(1)

	restore := setMockClient(cli)
	defer restore()

	c := newCluster([]string{"terminal-" + stringx.Rand()})
	defer func() {
		_ = connManager.RemoveResourceIf(c.key, cli)
	}()

	listener := NewMockUpdateListener(ctrl)
	monitored := make(chan error, 1)
	go func() {
		monitored <- c.monitor("any", listener)
	}()

	select {
	case err := <-monitored:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "permission denied")
	case <-time.After(2 * time.Second):
		t.Fatal("monitor did not report the terminal error")
	}

	c.lock.Lock()
	listeners := len(c.listeners["any"])
	c.lock.Unlock()
	assert.Equal(t, 0, listeners, "failed monitor must not leave an orphan listener")
}

// TestCluster_LoadInterruptsTransientRetries covers that a transient Get
// failure is retried, and that a replaced generation interrupts the cooldown
// instead of waiting it out.
func TestCluster_LoadInterruptsTransientRetries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cli := NewMockEtcdClient(ctrl)
	cli.EXPECT().Ctx().Return(context.Background()).AnyTimes()
	attempted := make(chan struct{}, 4)
	cli.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			attempted <- struct{}{}
			return nil, status.Error(codes.Unavailable, "etcdserver: no leader")
		}).AnyTimes()

	c := &cluster{
		done:   make(chan lang.PlaceholderType),
		values: make(map[string]map[string]string),
	}

	loaded := make(chan struct{})
	go func() {
		_, _ = c.load(c.done, cli, "any")
		close(loaded)
	}()

	select {
	case <-attempted:
	case <-time.After(2 * time.Second):
		t.Fatal("load did not attempt a Get")
	}

	start := time.Now()
	close(c.done)

	select {
	case <-loaded:
		// A plain sleep would take the whole coolDownInterval.
		assert.True(t, time.Since(start) < coolDownInterval/2, "load did not interrupt the cooldown")
	case <-time.After(2 * time.Second):
		t.Fatal("load did not stop after the generation was replaced")
	}
}
