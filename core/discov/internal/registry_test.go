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
	c.load(cli, "any")
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
			var wg sync.WaitGroup
			wg.Add(1)
			c := &cluster{
				listeners: make(map[string][]UpdateListener),
				values:    make(map[string]map[string]string),
			}
			listener := NewMockUpdateListener(ctrl)
			c.listeners["any"] = []UpdateListener{listener}
			listener.EXPECT().OnAdd(gomock.Any()).Do(func(kv KV) {
				assert.Equal(t, "hello", kv.Key)
				assert.Equal(t, "world", kv.Val)
				wg.Done()
			}).MaxTimes(1)
			listener.EXPECT().OnDelete(gomock.Any()).Do(func(_ interface{}) {
				wg.Done()
			}).MaxTimes(1)
			go c.watch(cli, "any", 0)
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
			wg.Wait()
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
			c.watch(cli, "any", 0)
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
	c.watch(cli, "any", 0)
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
		c.watch(cli, "any", 10)
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
		c.load(cli, "any")
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
		c.watch(deadCli, "any", 0)
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
