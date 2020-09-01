package main

import (
	"flag"
	"log"
)

var (
	// If informed, executes the log verifier script on log 'checkDir' instead of executing log files.
	checkDir  string
	outThrDir string

	logStrategy    int
	beelogInterval int
)

func init() {
	flag.StringVar(&checkDir, "check", "", "inform wheter log files shoul be verified after executing")
	flag.StringVar(&outThrDir, "output", ".", "set location to output throughput, defaults to .")
	flag.IntVar(&logStrategy, "log", 2, "set the desired log scenario, where (0: NotLog, 1: TradLog, 2: Beelog), defaults to the later")
	flag.IntVar(&beelogInterval, "interval", 1000, "set beelog log interval, defaults to 1000")
}

func main() {
	flag.Parse()
	if checkDir != "" {
		err := checkLocalLogs()
		if err != nil {
			log.Fatalln("failed logs verification with err: '", err.Error(), "'")
		}
		return
	}

	ls, ok := isValidLogStrategy()
	if !ok {
		log.Fatalln("unknown log strategy provided")
	}

	_, err := NewExecutor(ls)
	if err != nil {
		log.Fatalln("couldnt init log executor with err:", err.Error())
	}
}

func isValidLogStrategy() (LogStrat, bool) {
	// TODO: ...
	return 0, false
}
