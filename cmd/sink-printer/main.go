package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/relayer"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout io.Writer, stderr io.Writer) int {
	var (
		listenAddr string
		raw        bool
	)

	flags := flag.NewFlagSet(name(), flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&listenAddr, "listen-addr", "127.0.0.1:18080", "address to listen on")
	flags.BoolVar(&raw, "raw", false, "print raw envelope json after each summary line")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	logger := slog.New(slog.NewTextHandler(stderr, nil))
	printSink := &printer{
		output: stdout,
		raw:    raw,
		now: func() time.Time {
			return time.Now().UTC()
		},
		seen: make(map[string]relayer.Ack),
	}

	server := newServer(listenAddr, newMux(printSink))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("sink printer listening", "addr", listenAddr, "raw", raw)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("sink printer exited", "error", err)
		return 1
	}
	return 0
}

func name() string {
	return "sink-printer"
}
