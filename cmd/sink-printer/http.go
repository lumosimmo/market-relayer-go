package main

import "net/http"

func newMux(printSink *printer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.Handle("/publish", printSink)
	return mux
}

func newServer(listenAddr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:    listenAddr,
		Handler: handler,
	}
}
