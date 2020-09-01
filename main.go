package main

import (
	"flag"
	"log"
)

var (
	// If informed, executes the log verifier script on log 'checkDir' instead of executing log files.
	checkDir string

	inputFile string
	outThrDir string
	logsDir   string

	logStrategy    int
	beelogInterval int
)

func init() {
	flag.StringVar(&inputFile, "input", "", "set the input log file to be loaded and executed")
	flag.StringVar(&checkDir, "check", "", "inform a check location to switch execution between the log verifier and executor, defaults to the later")
	flag.StringVar(&outThrDir, "output", ".", "set location to output throughput, defaults to .")
	flag.StringVar(&logsDir, "logfolder", "/tmp/", "set location to persist log files, defaults to /tmp/")

	flag.IntVar(&logStrategy, "logstrat", 2, "set the desired log scenario, where (0: NotLog, 1: TradLog, 2: Beelog), defaults to the later")
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

	exec, err := NewExecutor(ls)
	if err != nil {
		log.Fatalln("couldnt init log executor, failed with err:", err.Error())
	}
	if err = exec.loadCommandLog(inputFile); err != nil {
		log.Fatalln("couldnt load command log, failed with err:", err.Error())
	}

	if err = exec.runLoadedLog(); err != nil {
		log.Fatalln("failed during log execution, err:", err.Error())
	}
	exec.shutdown()
}

func isValidLogStrategy() (LogStrat, bool) {
	return LogStrat(logStrategy), (0 <= logStrategy && logStrategy < 3)
}
