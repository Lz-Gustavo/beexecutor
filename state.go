package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/Lz-Gustavo/beelog"
	bl "github.com/Lz-Gustavo/beelog"
	"github.com/Lz-Gustavo/beelog/pb"
	"github.com/golang/protobuf/proto"
)

// Executor ...
type Executor struct {
	state  map[string][]byte
	cancel context.CancelFunc

	trad    bool
	logFile *os.File
	ct      *beelog.ConcTable

	count   uint32 // atomic
	t       *time.Ticker
	thrFile *os.File
}

// NewExecutor ...
func NewExecutor() *Executor {
	// TODO: initialize with new set of attributes
	m := &Executor{
		state: make(map[string][]byte, 0),
	}
	return m
}

// InstallRecovExecutorFromReader ...
func (ex *Executor) installStateFromReader(rd io.Reader) error {
	cmds, err := bl.UnmarshalLogFromReader(rd)
	if err != nil {
		return err
	}

	for _, cmd := range cmds {
		ex.runCommand(&cmd)
		err = ex.logCommand(&cmd)
		if err != nil {
			return err
		}
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
}

func (ex *Executor) logCommand(cmd *pb.Command) error {
	if ex.trad {
		rawCmd, err := proto.Marshal(cmd)
		if err != nil {
			return err
		}

		err = binary.Write(ex.logFile, binary.BigEndian, int32(len(rawCmd)))
		if err != nil {
			return err
		}

		_, err = ex.logFile.Write(rawCmd)
		if err != nil {
			return err
		}

	} else {
		if err := ex.ct.Log(*cmd); err != nil {
			return err
		}
	}
	atomic.AddUint32(&ex.count, 1)
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

// checkLogCountingDiffKeys executes received commands on mock state, returning the
// number of different keys identified
func (ex *Executor) checkLogCountingDiffKeys(log []pb.Command) int {
	diff := 0
	for _, cmd := range log {
		if _, ok := ex.state[cmd.Key]; !ok {
			diff++
		}

		switch cmd.Op {
		case pb.Command_SET:
			ex.state[cmd.Key] = []byte(cmd.Value)

		case pb.Command_GET:
			// insert an empty value to count READ operations on unique keys
			ex.state[cmd.Key] = []byte{}

		default:
			break
		}
	}
	return diff
}
