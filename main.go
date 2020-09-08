package main

import (
	"flag"
	"log"
)

var (
	// If informed, executes the log verifier script on log 'checkDir' instead of executing
	// log files.
	checkDir string

	// Indicates whether retrieved logs should be sorted before being sequentially applied
	// during verification.
	sortLogs bool

	// Input file of commands, throughput output and logs destinations.
	inputFile, outThrDir, logsDir string

	// Sets the desired log strategy and configures beelog.
	logStrategy, beelogInterval, beelogConcLevel int

	// SyncIO during log persistence, utilized while evaluating catastrophic fault models.
	syncIO bool

	// Enables output of latency metrics during execution.
	latencyMeasurement bool
)

func init() {
	flag.StringVar(&checkDir, "check", "", "inform a check location to switch execution between the log verifier and executor, defaults to the later")
	flag.BoolVar(&sortLogs, "sort", false, "set if logs should be sorted before being sequentially applied during verification, only if -check=/path/ is informed")
	flag.StringVar(&inputFile, "input", "", "set the input log file to be loaded and executed")
	flag.StringVar(&outThrDir, "output", "./", "set location to output throughput")
	flag.StringVar(&logsDir, "logfolder", "/tmp/", "set location to persist log files")
	flag.IntVar(&logStrategy, "logstrat", 2, "set the desired log scenario, where (0: NotLog, 1: TradLog, 2: Beelog)")
	flag.IntVar(&beelogInterval, "interval", 1000, "set beelog log interval, defaults to 1000")
	flag.IntVar(&beelogConcLevel, "conclevel", 2, "set beelog concurrency level, number of table views")
	flag.BoolVar(&syncIO, "sync", false, "enables syncIO during log persistence")
	flag.BoolVar(&latencyMeasurement, "latency", false, "enables latency measurement during execution")
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

	if inputFile == "" {
		log.Fatalln("must inform an input log file, run with: './beexecutor -input=/path/to/file.log'")
	}

	ls, ok := isValidLogStrategy()
	if !ok {
		log.Fatalln("unknown log strategy provided")
	}

	exec, err := NewExecutor(ls)
	if err != nil {
		log.Fatalln("could not init log executor, failed with err:", err.Error())
	}
	if err = exec.loadCommandLog(inputFile); err != nil {
		log.Fatalln("could not load command log, failed with err:", err.Error())
	}

	if err = exec.runLoadedLog(); err != nil {
		log.Fatalln("failed during log execution, err:", err.Error())
	}
	exec.shutdown()
}

func isValidLogStrategy() (LogStrat, bool) {
	return LogStrat(logStrategy), (0 <= logStrategy && logStrategy < 3)
}
