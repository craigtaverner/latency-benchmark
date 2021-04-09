package main

import (
	_ "go.uber.org/automaxprocs"
	"neo4j-client-benchmark/pkg/benchmark"
)

func main() {
	benchmark.NewServer().Run()
}
