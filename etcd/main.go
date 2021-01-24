package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"
)

var (
	// Input file of commands, throughput output and logs destinations.
	inputFile string

	// IP address of etcd's server.
	// TODO: parse a comma-separated list of strings if multiples endpoints are necessary.
	etcdIPAddr string

	// Sets a maximum execution time for log processing. Input loading is excluded from counting.
	expTimeoutInMinutes int
)

func init() {
	flag.StringVar(&inputFile, "input", "", "set the input log file to be loaded and executed")
	flag.StringVar(&etcdIPAddr, "etcd", defaultEtcdIP, "set the etcd server node, defaults to 127.0.0.1 (localhost)")
	flag.IntVar(&expTimeoutInMinutes, "timeout", 0, "if greater than zero, sets a maximum execution time")
}

func main() {
	flag.Parse()
	if inputFile == "" {
		log.Fatalln("must inform an input log file, run with: './etcd -input=/path/to/file.log'")
	}

	// TODO: currently not canceling the same context as passed to client procedures,
	// refac later.
	ctx := context.Background()

	ec, err := NewEtcdClient(ctx)
	if err != nil {
		log.Fatalln("could not init etcd client, err:", err.Error())
	}

	if err = ec.loadCommandLog(inputFile); err != nil {
		log.Fatalln("could not load command log, err:", err.Error())
	}

	if t := expTimeoutInMinutes; t > 0 {
		go func() {
			time.Sleep(time.Duration(t) * time.Minute)
			ec.shutdown()
			os.Exit(0)
		}()
	}

	err = ec.runLoadedLog(ctx)
	if err != nil {
		log.Fatalln("failed during client execution, err:", err.Error())
	}
	ec.shutdown()
}
