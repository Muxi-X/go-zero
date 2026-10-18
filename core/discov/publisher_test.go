package discov

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/discov/internal"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stringx"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	logx.Disable()
}

func TestPublisher_register(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const id = 1
	cli := internal.NewMockEtcdClient(ctrl)
	restore := setMockClient(cli)
	defer restore()
	cli.EXPECT().Ctx().AnyTimes()
	cli.EXPECT().Grant(gomock.Any(), timeToLive).Return(&clientv3.LeaseGrantResponse{
		ID: id,
	}, nil)
	cli.EXPECT().Put(gomock.Any(), makeEtcdKey("thekey", id), "thevalue", gomock.Any())
	pub := NewPublisher(nil, "thekey", "thevalue",
		WithPubEtcdAccount(stringx.Rand(), "bar"))
	_, err := pub.register(cli)
	assert.Nil(t, err)
}

func TestPublisher_registerWithId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const id = 2
	cli := internal.NewMockEtcdClient(ctrl)
	restore := setMockClient(cli)
	defer restore()
	cli.EXPECT().Ctx().AnyTimes()
	cli.EXPECT().Grant(gomock.Any(), timeToLive).Return(&clientv3.LeaseGrantResponse{
		ID: 1,
	}, nil)
	cli.EXPECT().Put(gomock.Any(), makeEtcdKey("thekey", id), "thevalue", gomock.Any())
	pub := NewPublisher(nil, "thekey", "thevalue", WithId(id))
	_, err := pub.register(cli)
	assert.Nil(t, err)
}

func TestPublisher_registerError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cli := internal.NewMockEtcdClient(ctrl)
	restore := setMockClient(cli)
	defer restore()
	cli.EXPECT().Ctx().AnyTimes()
	cli.EXPECT().Grant(gomock.Any(), timeToLive).Return(nil, errors.New("error"))
	pub := NewPublisher(nil, "thekey", "thevalue")
	val, err := pub.register(cli)
	assert.NotNil(t, err)
	assert.Equal(t, clientv3.NoLease, val)
}

func TestPublisher_revoke(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const id clientv3.LeaseID = 1
	cli := internal.NewMockEtcdClient(ctrl)
	restore := setMockClient(cli)
	defer restore()
	cli.EXPECT().Ctx().AnyTimes()
	cli.EXPECT().Revoke(gomock.Any(), id)
	pub := NewPublisher(nil, "thekey", "thevalue")
	pub.lease = id
	pub.revoke(cli)
}

func TestPublisher_revokeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const id clientv3.LeaseID = 1
	cli := internal.NewMockEtcdClient(ctrl)
	restore := setMockClient(cli)
	defer restore()
	cli.EXPECT().Ctx().AnyTimes()
	cli.EXPECT().Revoke(gomock.Any(), id).Return(nil, errors.New("error"))
	pub := NewPublisher(nil, "thekey", "thevalue")
	pub.lease = id
	pub.revoke(cli)
}

func TestPublisher_keepAliveAsyncError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const id clientv3.LeaseID = 1
	cli := internal.NewMockEtcdClient(ctrl)
	restore := setMockClient(cli)
	defer restore()
	cli.EXPECT().Ctx().AnyTimes()
	cli.EXPECT().KeepAlive(gomock.Any(), id).Return(nil, errors.New("error"))
	pub := NewPublisher(nil, "thekey", "thevalue")
	pub.lease = id
	assert.NotNil(t, pub.keepAliveAsync(cli))
}

func TestPublisher_keepAliveAsyncQuit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const id clientv3.LeaseID = 1
	cli := internal.NewMockEtcdClient(ctrl)
	cli.EXPECT().ActiveConnection()
	cli.EXPECT().Close()
	defer cli.Close()
	cli.ActiveConnection()
	restore := setMockClient(cli)
	defer restore()
	cli.EXPECT().Ctx().AnyTimes()
	cli.EXPECT().KeepAlive(gomock.Any(), id)
	var wg sync.WaitGroup
	wg.Add(1)
	cli.EXPECT().Revoke(gomock.Any(), id).Do(func(_, _ interface{}) {
		wg.Done()
	})
	pub := NewPublisher(nil, "thekey", "thevalue")
	pub.lease = id
	pub.Stop()
	assert.Nil(t, pub.keepAliveAsync(cli))
	wg.Wait()
}

func TestPublisher_keepAliveAsyncPause(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const id clientv3.LeaseID = 1
	cli := internal.NewMockEtcdClient(ctrl)
	restore := setMockClient(cli)
	defer restore()
	cli.EXPECT().Ctx().AnyTimes()
	cli.EXPECT().KeepAlive(gomock.Any(), id)
	pub := NewPublisher(nil, "thekey", "thevalue")
	var wg sync.WaitGroup
	wg.Add(1)
	cli.EXPECT().Revoke(gomock.Any(), id).Do(func(_, _ interface{}) {
		pub.Stop()
		wg.Done()
	})
	pub.lease = id
	assert.Nil(t, pub.keepAliveAsync(cli))
	pub.Pause()
	wg.Wait()
}

func TestPublisher_Resume(t *testing.T) {
	publisher := new(Publisher)
	publisher.resumeChan = make(chan lang.PlaceholderType)
	go func() {
		publisher.Resume()
	}()
	go func() {
		time.Sleep(time.Minute)
		t.Fail()
	}()
	<-publisher.resumeChan
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

func TestPublisher_keepAliveAsyncRefreshesClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		firstLease  clientv3.LeaseID = 1
		secondLease clientv3.LeaseID = 2
	)
	endpoints := []string{"auth-expired-" + stringx.Rand()}
	firstConn := createMockConn(t)
	defer firstConn.Close()
	secondConn := createMockConn(t)
	defer secondConn.Close()

	firstCli := internal.NewMockEtcdClient(ctrl)
	secondCli := internal.NewMockEtcdClient(ctrl)
	restore := setMockClients(firstCli, secondCli)
	defer restore()

	firstKeepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)
	secondKeepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)
	// The cached clients start state watchers that exit on ctx cancellation,
	// so each mock client needs a cancellable context.
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()

	firstCli.EXPECT().Ctx().Return(firstCtx).AnyTimes()
	firstCli.EXPECT().ActiveConnection().Return(firstConn).AnyTimes()
	firstCli.EXPECT().Grant(gomock.Any(), timeToLive).Return(&clientv3.LeaseGrantResponse{
		ID: firstLease,
	}, nil)
	firstCli.EXPECT().Put(gomock.Any(), makeEtcdKey("thekey", int64(firstLease)), "thevalue",
		gomock.Any())
	firstCli.EXPECT().KeepAlive(gomock.Any(), firstLease).Return(firstKeepAliveChan, nil)
	firstCli.EXPECT().Revoke(gomock.Any(), firstLease)
	firstCli.EXPECT().Close().Return(errors.New("close error"))

	var wg sync.WaitGroup
	wg.Add(1)
	secondKeepAliveStarted := make(chan struct{})
	secondCli.EXPECT().Ctx().Return(secondCtx).AnyTimes()
	secondCli.EXPECT().ActiveConnection().Return(secondConn).AnyTimes()
	secondCli.EXPECT().Grant(gomock.Any(), timeToLive).Return(&clientv3.LeaseGrantResponse{
		ID: secondLease,
	}, nil)
	secondCli.EXPECT().Put(gomock.Any(), makeEtcdKey("thekey", int64(secondLease)), "thevalue",
		gomock.Any())
	secondCli.EXPECT().KeepAlive(gomock.Any(), secondLease).DoAndReturn(func(context.Context,
		clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
		close(secondKeepAliveStarted)
		return secondKeepAliveChan, nil
	})
	secondCli.EXPECT().Revoke(gomock.Any(), secondLease).Do(func(_, _ interface{}) {
		wg.Done()
	})
	secondCli.EXPECT().Close()

	pub := NewPublisher(endpoints, "thekey", "thevalue")
	assert.Nil(t, pub.KeepAlive())
	close(firstKeepAliveChan)

	select {
	case <-secondKeepAliveStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for publisher to refresh the client")
	}

	pub.Stop()
	wg.Wait()
	assert.NoError(t, internal.GetRegistry().InvalidateConn(endpoints, secondCli))
}

// TestPublisher_ResumeKeepsSharedClient covers that resuming a publisher
// reuses the cached etcd client. Invalidating it on resume would close the
// client shared by every watcher and publisher on the same endpoints, so a
// Close call would fail this test as unexpected.
func TestPublisher_ResumeKeepsSharedClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const lease clientv3.LeaseID = 1

	cli := internal.NewMockEtcdClient(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli.EXPECT().Ctx().Return(ctx).AnyTimes()
	conn := createMockConn(t)
	defer conn.Close()
	cli.EXPECT().ActiveConnection().Return(conn).AnyTimes()

	firstCh := make(chan *clientv3.LeaseKeepAliveResponse)
	secondCh := make(chan *clientv3.LeaseKeepAliveResponse)
	resumed := make(chan struct{})
	revoked := make(chan struct{}, 4)
	cli.EXPECT().Grant(gomock.Any(), timeToLive).Return(&clientv3.LeaseGrantResponse{
		ID: lease,
	}, nil).Times(2)
	cli.EXPECT().Put(gomock.Any(), gomock.Any(), "thevalue", gomock.Any()).Times(2)
	gomock.InOrder(
		cli.EXPECT().KeepAlive(gomock.Any(), lease).Return(firstCh, nil),
		cli.EXPECT().KeepAlive(gomock.Any(), lease).DoAndReturn(func(context.Context,
			clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
			close(resumed)
			return secondCh, nil
		}),
	)
	cli.EXPECT().Revoke(gomock.Any(), lease).DoAndReturn(func(context.Context,
		clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
		revoked <- struct{}{}
		return &clientv3.LeaseRevokeResponse{}, nil
	}).AnyTimes()

	restore := setMockClient(cli)
	defer restore()

	pub := NewPublisher([]string{"resume-" + stringx.Rand()}, "thekey", "thevalue")
	assert.Nil(t, pub.KeepAlive())

	pub.Pause()
	pub.Resume()

	select {
	case <-resumed:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the publisher to resume")
	}

	select {
	case <-revoked: // the lease revoked on pause
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the pause to revoke the lease")
	}

	pub.Stop()

	select {
	case <-revoked: // the lease revoked on stop
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for stop to revoke the lease")
	}
}
