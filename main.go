package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

var (
	// TODO: describe...
	execMode int

	// If informed, executes the log verifier script on log 'checkDir' instead of executing
	// log files.
	checkDir string

	// Indicates whether retrieved logs should be sorted before being sequentially applied
	// during verification.
	sortLogs bool

	// Input file of commands, throughput output and logs destinations.
	inputFile, outThrDir, logsDir string

	// Enables ParallelIO by informing a second log dir to alternate log files persistence.
	secondaryLogDir string

	// Sets the desired log strategy and configures beelog.
	logStrategy, persistInterval, beelogConcLevel int

	// SyncIO during log persistence, utilized while evaluating catastrophic fault models.
	syncIO bool

	// Enables output of latency metrics during execution.
	latencyMeasurement bool

	// Sets a maximum execution time for log processing. Input loading is excluded from counting.
	expTimeoutInMinutes int
)

func init() {
	// TODO: adequate others description and runExec script for new init flag
	flag.IntVar(&execMode, "mode", 0, "set the desired execution mode, where (0: log verifier, 1: log executor, 2: etcd client)")

	flag.StringVar(&checkDir, "check", "", "inform a check location to switch execution between the log verifier and executor, defaults to the later")
	flag.BoolVar(&sortLogs, "sort", false, "set if logs should be sorted before being sequentially applied during verification, only if -check=/path/ is informed")
	flag.StringVar(&inputFile, "input", "", "set the input log file to be loaded and executed")
	flag.StringVar(&outThrDir, "output", "./", "set location to output throughput")
	flag.StringVar(&logsDir, "logfolder", "/tmp/", "set location to persist log files")
	flag.IntVar(&logStrategy, "logstrat", 2, "set the desired log scenario, where (0: NotLog, 1: TradLog, 2: Beelog, 3: TradBatch)")
	flag.IntVar(&persistInterval, "interval", 1000, "set the log persistence interval for beelog or tradbatch, defaults to 1000")
	flag.IntVar(&beelogConcLevel, "conclevel", 2, "set beelog concurrency level, number of table views")
	flag.BoolVar(&syncIO, "sync", false, "enables syncIO during log persistence")
	flag.BoolVar(&latencyMeasurement, "latency", false, "enables latency measurement during execution")
	flag.IntVar(&expTimeoutInMinutes, "timeout", 0, "if greater than zero, sets a maximum execution time")
	flag.StringVar(&secondaryLogDir, "secdisk", "", "if informed, enables parallel log persistence by alternating between two log file destinations")
}

func main() {
	flag.Parse()
	if !isValidExecMode() {
		log.Fatalln("unknow exec mode '", execMode, "' provided.")
	}

	switch execMode {
	case 0:
		if checkDir == "" {
			log.Fatalln("must inform a folder location to run the log verifier, run with: './beexecutor -check=/path/to/folder/'")
		}

		if err := checkLocalLogs(); err != nil {
			log.Fatalln("failed logs verification with err: '", err.Error(), "'")
		}
		return

	case 1:
		if err := runExecutor(); err != nil {
			log.Fatalln("failed log executor with err: '", err.Error(), "'")
		}
		return

	case 2:
		// TODO: run etcd client procedure
	}
}

func runExecutor() error {
	if inputFile == "" {
		return fmt.Errorf("must inform an input log file, run with: './beexecutor -input=/path/to/file.log'")
	}

	ls, ok := isValidLogStrategy()
	if !ok {
		return fmt.Errorf("unknown log strategy provided")
	}

	exec, err := NewExecutor(ls)
	if err != nil {
		return fmt.Errorf("could not init log executor, failed with err: %s", err.Error())
	}
	if err = exec.loadCommandLog(inputFile); err != nil {
		return fmt.Errorf("could not load command log, failed with err: %s", err.Error())
	}

	if t := expTimeoutInMinutes; t > 0 {
		go func() {
			time.Sleep(time.Duration(t) * time.Minute)
			exec.shutdown()
			os.Exit(0)
		}()
	}

	err = exec.runLoadedLog()
	if err != nil {
		return err
	}

	exec.shutdown()
	return nil
}

func isValidLogStrategy() (LogStrat, bool) {
	return LogStrat(logStrategy), (0 <= logStrategy && logStrategy < 4)
}

func isValidExecMode() bool {
	return 0 <= execMode && execMode < 3
}
