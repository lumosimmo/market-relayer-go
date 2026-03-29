package app

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/api"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
)

type Runtime struct {
	Config   *config.Config
	Metadata RuntimeMetadata
	Store    runtimeStore
	Sink     relayer.Sink
	Metrics  *metrics.Registry
	API      *api.Server
	Loops    []*relayer.Loop
	workers  []*marketWorker
	state    *runtimeState
}

func Run(parent context.Context, options Options) error {
	service, err := Bootstrap(parent, options)
	if err != nil {
		return err
	}
	return Serve(parent, service)
}

func Serve(ctx context.Context, service *Runtime) error {
	loopCtx, cancel := context.WithCancel(ctx)
	loopErrors := service.StartLoops(loopCtx)
	return runService(ctx, loopErrors, service.API.Errors(), coordinatedClose(cancel, loopErrors, service.Config.API.ShutdownTimeout, func() error {
		return closeRuntime(service)
	}))
}

func runService(
	ctx context.Context,
	loopErrors <-chan error,
	apiErrors <-chan error,
	closeFn func() error,
) error {
	var closeOnce sync.Once
	shutdown := func() error {
		var err error
		closeOnce.Do(func() {
			if closeFn != nil {
				err = closeFn()
			}
		})
		return err
	}

	for loopErrors != nil || apiErrors != nil {
		select {
		case <-ctx.Done():
			return shutdown()
		case err, ok := <-loopErrors:
			if !ok {
				return shutdown()
			}
			return errors.Join(err, shutdown())
		case err, ok := <-apiErrors:
			if !ok {
				return shutdown()
			}
			return errors.Join(err, shutdown())
		}
	}

	return shutdown()
}

func (runtime *Runtime) Close(ctx context.Context) error {
	return errors.Join(
		runtime.API.Close(ctx),
		runtime.Sink.Close(),
		runtime.Store.Close(),
	)
}

func (runtime *Runtime) StartLoops(ctx context.Context) <-chan error {
	errCh := make(chan error, len(runtime.Loops))
	if len(runtime.Loops) == 0 {
		close(errCh)
		return errCh
	}
	runtime.state.markLoopsStarted()

	var wait sync.WaitGroup
	for index, loop := range runtime.Loops {
		loop := loop
		worker := (*marketWorker)(nil)
		if index < len(runtime.workers) {
			worker = runtime.workers[index]
		}
		market := loop.Status().Market
		wait.Add(1)
		go func() {
			defer wait.Done()
			defer runtime.state.markLoopStopped(market)
			var err error
			if worker != nil {
				err = worker.Run(ctx, runtime.state)
			} else {
				err = loop.Run(ctx)
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}
	go func() {
		wait.Wait()
		close(errCh)
	}()
	return errCh
}

func closeRuntime(runtime *Runtime) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), runtime.Config.API.ShutdownTimeout)
	defer cancel()
	return runtime.Close(shutdownCtx)
}

func coordinatedClose(
	cancel context.CancelFunc,
	loopErrors <-chan error,
	timeout time.Duration,
	closeFn func() error,
) func() error {
	return func() error {
		if cancel != nil {
			cancel()
		}
		waitErr := waitForLoopShutdown(loopErrors, timeout)
		if closeFn == nil {
			return waitErr
		}
		return errors.Join(waitErr, closeFn())
	}
}

func waitForLoopShutdown(loopErrors <-chan error, timeout time.Duration) error {
	if loopErrors == nil {
		return nil
	}

	drained := make(chan struct{})
	go func() {
		for range loopErrors {
		}
		close(drained)
	}()

	if timeout <= 0 {
		<-drained
		return nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-drained:
		return nil
	case <-timer.C:
		return errors.New("app: timed out waiting for loops to stop")
	}
}
