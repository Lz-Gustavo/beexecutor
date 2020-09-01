package main

import (
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
)

// Executor ...
type Executor struct {
	state  map[string][]byte
	cmds   *[]pb.Command
	cancel context.CancelFunc

	logT    LogStrat
	logFile *os.File
	ct      *beelog.ConcTable

	count   uint32 // atomic
	t       *time.Ticker
	thrFile *os.File
}

// NewExecutor ...
func NewExecutor(ls LogStrat) (*Executor, error) {
	fn := outThrDir + "thr-int-" + strconv.Itoa(beelogInterval) + ".out"
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

	case TradLog:
		fn := logsDir + "logfile.log"
		ex.logFile, err = os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}

	case Beelog:
		cfg := &beelog.LogConfig{
			Alg:     beelog.IterConcTable,
			Tick:    beelog.Interval,
			Period:  uint32(beelogInterval),
			KeepAll: true,
			Fname:   logsDir + "beelog.log",
		}
		ex.ct, err = beelog.NewConcTableWithConfig(ctx, beelogConcLevel, cfg)
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
	atomic.AddUint32(&ex.count, 1)
}

func (ex *Executor) logCommand(cmd *pb.Command) error {
	switch ex.logT {
	case NotLog:
		return nil

	case TradLog:
		raw, err := proto.Marshal(cmd)
		if err != nil {
			return err
		}

		err = binary.Write(ex.logFile, binary.BigEndian, int32(len(raw)))
		if err != nil {
			return err
		}

		_, err = ex.logFile.Write(raw)
		if err != nil {
			return err
		}

	case Beelog:
		if err := ex.ct.Log(*cmd); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown log strategy '%d' provided", ex.logT)
	}
	return nil
}

func (ex *Executor) monitorThroughput(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ex.t.C:
			t := atomic.SwapUint32(&ex.count, 0)
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
	case TradLog:
		ex.logFile.Close()

	case Beelog:
		ex.ct.Shutdown()
	}
	ex.thrFile.Close()
}