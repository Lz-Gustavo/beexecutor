package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	bl "github.com/Lz-Gustavo/beelog"
	"github.com/Lz-Gustavo/beelog/pb"
)

const (
	disktradLogs = "logfile*.log"
	beelogLogs   = "beelog*.log"
)

// LogVerifier ...
type LogVerifier struct {
	state map[string][]byte
}

// NewLogVerifier ...
func NewLogVerifier() *LogVerifier {
	return &LogVerifier{
		state: make(map[string][]byte, 0),
	}
}

// checkLogCountingDiffKeys executes received commands on mock state, returning the
// number of different keys identified.
func (lv *LogVerifier) checkLogCountingDiffKeys(log []pb.Command) int {
	diff := 0
	for _, cmd := range log {
		if _, ok := lv.state[cmd.Key]; !ok {
			diff++
		}

		switch cmd.Op {
		case pb.Command_SET:
			lv.state[cmd.Key] = []byte(cmd.Value)

		case pb.Command_GET:
			// insert an empty value to count READ operations on unique keys
			lv.state[cmd.Key] = []byte{}

		default:
			break
		}
	}
	return diff
}

func checkLocalLogs() error {
	fmt.Println(
		"=========================",
		"\nrunning log verifier on", checkDir, "...",
		"\n=========================",
	)

	dlogs, err := filepath.Glob(checkDir + disktradLogs)
	if err != nil {
		return err
	}
	dlogs = rmvRepetitiveLogs(dlogs)

	blogs, err := filepath.Glob(checkDir + beelogLogs)
	if err != nil {
		return err
	}
	blogs = rmvRepetitiveLogs(blogs)

	if sortLogs {
		// sorts by lenght and lexicographically for equal len
		sort.Sort(byLenAlpha(dlogs))
		sort.Sort(byLenAlpha(blogs))
	}

	logs := [][]string{dlogs, blogs}
	names := []string{"disktrad", "beelog"}

	for i, l := range logs {
		var (
			diff, nCmds int
			totalSize   int64
		)
		state := NewLogVerifier()

		for _, fn := range l {
			fd, err := os.OpenFile(fn, os.O_RDONLY, 0400)
			if err != nil && err != io.EOF {
				return fmt.Errorf("failed while opening log '%s', err: '%s'", fn, err.Error())
			}

			info, err := fd.Stat()
			if err != nil {
				fd.Close()
				return err
			}
			totalSize += info.Size()

			cmds, err := bl.UnmarshalLogFromReader(fd)
			if err != nil {
				fd.Close()
				return err
			}
			dk := state.checkLogCountingDiffKeys(cmds)

			nCmds += len(cmds)
			diff += dk
			fd.Close()
		}

		fmt.Println(
			"=========================",
			"\nFinished installing logs for", names[i],
			"\nNum of different logs:   ", len(l),
			"\nNum of commands:         ", nCmds,
			"\nNum of unique keys:      ", diff,
			"\nTotal state size (bytes):", totalSize,
		)
	}
	return nil
}

// rmvRepetitiveLogs identifies the first node identifier within log filenames,
// then ignores logs from all different nodes.
func rmvRepetitiveLogs(logs []string) []string {
	if len(logs) < 1 {
		return nil
	}
	uniques := make([]string, 0, len(logs))

	// e.g. beelog-node1.1000.log -> [beelog-node1., 1000.log]
	split := strings.SplitAfterN(logs[0], ".", 2)

	// e.g. beelog-node1. -> node1.
	id := strings.SplitAfter(split[0], "-")[1]

	for _, l := range logs {
		if strings.Contains(l, id) {
			uniques = append(uniques, l)
		}
	}
	return uniques
}

type byLenAlpha []string

func (a byLenAlpha) Len() int      { return len(a) }
func (a byLenAlpha) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byLenAlpha) Less(i, j int) bool {
	// lenght order prio
	if len(a[i]) < len(a[j]) {
		return true
	}
	// alphabetic
	if len(a[i]) == len(a[j]) {
		return strings.Compare(a[i], a[j]) == -1
	}
	return false
}
