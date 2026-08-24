package internal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"google.golang.org/grpc/connectivity"
)

func TestStateWatcher_watch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	watcher := newStateWatcher()
	var wg sync.WaitGroup
	wg.Add(1)
	watcher.addListener(func() {
		wg.Done()
	})
	conn := NewMocketcdConn(ctrl)
	conn.EXPECT().GetState().Return(connectivity.Ready)
	conn.EXPECT().GetState().Return(connectivity.TransientFailure)
	conn.EXPECT().GetState().Return(connectivity.Ready).AnyTimes()
	conn.EXPECT().WaitForStateChange(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ connectivity.State) bool {
			return ctx.Err() == nil
		}).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		watcher.watch(ctx, conn)
		close(done)
	}()
	wg.Wait()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watch did not exit after cancel")
	}
}
