package main

import "os"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, defaultCommandDeps()))
}

func Name() string {
	return "market-relayer"
}
