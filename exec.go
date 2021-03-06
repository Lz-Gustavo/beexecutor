package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Lz-Gustavo/beelog"
	bl "github.com/Lz-Gustavo/beelog"
	"github.com/Lz-Gustavo/beelog/pb"
	"github.com/golang/protobuf/proto"
)

// LogStrat ...
type LogStrat int8

const (
	// NotLog ...
	NotLog LogStrat = iota

	// TradLog ...
	TradLog

	// Beelog ...
	Beelog

	// TradBatch ...
	TradBatch
)

func configBeelog() *bl.LogConfig {
	return &bl.LogConfig{
		Alg:     beelog.IterConcTable,
		Sync:    syncIO,
		Measure: latencyMeasurement,
		Tick:    beelog.Interval,
		Period:  uint32(persistInterval),
		KeepAll: true,
		Fname:   logsDir + "beelog.log",
	}
}

// Executor ...
type Executor struct {
	state  map[string][]byte
	cmds   *[]pb.Command
	cancel context.CancelFunc

	logT    LogStrat
	logFile *os.File
	ct      *beelog.ConcTable

	cmdBuff    *bytes.Buffer
	batchCount int // TradBatch only
	batchLat   time.Time
	measuring  bool
	thrCount   uint32 // atomic

	t       *time.Ticker
	thrFile *os.File
	latFile *os.File // TradLog only
}

// NewExecutor ...
func NewExecutor(ls LogStrat) (*Executor, error) {
	fn := outThrDir + "thr-int-" + strconv.Itoa(persistInterval) + ".out"
	fd, err := os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	ctx, cn := context.WithCancel(context.Background())
	ex := &Executor{
		state:   make(map[string][]byte, 0),
		cancel:  cn,
		logT:    ls,
		t:       time.NewTicker(time.Second),
		thrFile: fd,
	}

	switch ls {
	case NotLog:
		break

	case TradLog, TradBatch:
		if ls == TradLog {
			persistInterval = 1
		}

		ex.cmdBuff = bytes.NewBuffer(nil)
		fn := logsDir + "logfile.log"
		flags := os.O_CREATE | os.O_TRUNC | os.O_WRONLY | os.O_APPEND
		if syncIO {
			flags = flags | os.O_SYNC
		}
		ex.logFile, err = os.OpenFile(fn, flags, 0600)
		if err != nil {
			return nil, err
		}

		// in compliance with the beelog log format definition, which allows the reutilization
		// of its marshal/unmarshal procedures
		_, err = fmt.Fprintf(ex.logFile, "%d\n%d\n%d\n", uint64(0), uint64(0), -1)
		if err != nil {
			return nil, err
		}

		if latencyMeasurement {
			// beelog also outputs to logsDir, following the standard for now
			fn := logsDir + "trad-" + strconv.Itoa(persistInterval) + "-latency.out"
			ex.latFile, err = os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
			if err != nil {
				return nil, err
			}
		}

	case Beelog:
		bc := configBeelog()
		if secondaryLogDir != "" {
			bc.ParallelIO = true
			bc.SecondFname = secondaryLogDir + "logfile.log"
		}

		ex.ct, err = beelog.NewConcTableWithConfig(ctx, beelogConcLevel, bc)
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("unknown log strategy '%d' provided", ls)
	}

	go ex.monitorThroughput(ctx)
	return ex, nil
}

func (ex *Executor) loadCommandLog(fn string) error {
	rd, err := os.OpenFile(fn, os.O_RDONLY, 0400)
	if err != nil {
		return err
	}

	cmds, err := bl.UnmarshalLogFromReader(rd)
	if err != nil {
		return err
	}
	ex.cmds = &cmds
	return nil
}

func (ex *Executor) runLoadedLog() error {
	if ex.cmds == nil {
		return fmt.Errorf("empty command log, load it first")
	}

	var err error
	for _, cmd := range *ex.cmds {
		err = ex.logCommand(&cmd)
		if err != nil {
			return err
		}
		ex.runCommand(&cmd)
	}
	return nil
}

// applyLog executes received commands on mock state.
func (ex *Executor) runCommand(cmd *pb.Command) {
	switch cmd.Op {
	case pb.Command_SET:
		ex.state[cmd.Key] = []byte(cmd.Value)

	default:
		break
	}
	atomic.AddUint32(&ex.thrCount, 1)
}

func (ex *Executor) logCommand(cmd *pb.Command) error {
	switch ex.logT {
	case NotLog:
		return nil

	case TradLog:
		return ex.logToFile(cmd)

	case TradBatch:
		return ex.batchLogToFile(cmd)

	case Beelog:
		if err := ex.ct.Log(*cmd); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown log strategy '%d' provided", ex.logT)
	}
	return nil
}

func (ex *Executor) batchLogToFile(cmd *pb.Command) error {
	// append command to buffer
	raw, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	err = binary.Write(ex.cmdBuff, binary.BigEndian, int32(len(raw)))
	if err != nil {
		return err
	}

	_, err = ex.cmdBuff.Write(raw)
	if err != nil {
		return err
	}

	ex.batchCount++
	if ex.batchCount >= persistInterval {
		_, err := ex.cmdBuff.WriteTo(ex.logFile)
		if err != nil {
			return err
		}

		if latencyMeasurement {
			dur := time.Since(ex.batchLat)
			if _, err = fmt.Fprintf(ex.latFile, "%d\n", dur); err != nil {
				return err
			}
			ex.measuring = false
		}
		ex.batchCount = 0
		ex.cmdBuff = bytes.NewBuffer(nil)

	} else if latencyMeasurement && !ex.measuring {
		ex.batchLat = time.Now()
		ex.measuring = true
	}
	return nil
}

func (ex *Executor) logToFile(cmd *pb.Command) error {
	st := time.Now()
	raw, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	// must write size and raw input in an unique flush, thats why cmdBuff is used
	// even on single command standard approach
	err = binary.Write(ex.cmdBuff, binary.BigEndian, int32(len(raw)))
	if err != nil {
		return err
	}

	_, err = ex.cmdBuff.Write(raw)
	if err != nil {
		return err
	}

	_, err = ex.cmdBuff.WriteTo(ex.logFile)
	if err != nil {
		return err
	}
	ex.cmdBuff = bytes.NewBuffer(nil)

	if latencyMeasurement {
		dur := time.Since(st)
		if _, err = fmt.Fprintf(ex.latFile, "%d\n", dur); err != nil {
			return err
		}
	}
	return nil
}

func (ex *Executor) monitorThroughput(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ex.t.C:
			t := atomic.SwapUint32(&ex.thrCount, 0)
			_, err := fmt.Fprintf(ex.thrFile, "%d\n", t)
			if err != nil {
				return err
			}
		}
	}
}

func (ex *Executor) shutdown() {
	// wait for throughput wrt
	time.Sleep(time.Second)

	ex.cancel()
	switch ex.logT {
	case TradLog, TradBatch:
		ex.logFile.Close()
		if latencyMeasurement {
			ex.latFile.Close()
		}

	case Beelog:
		ex.ct.Shutdown()
	}
	ex.thrFile.Close()
}
