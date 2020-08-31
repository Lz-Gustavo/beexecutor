package main

import (
	"flag"
)

var (
	// If informed, executes the log verifier script instead of and state requester.
	checkDir     string
	multipleLogs bool
)

func init() {
	flag.StringVar(&checkDir, "check", "", "inform wheter log files shoul be verified after executing")
	flag.BoolVar(&multipleLogs, "mult", false, "inform wheter multiple logs will be returned")
}

func main() {
	flag.Parse()
	// TODO: threat diff configs
}
