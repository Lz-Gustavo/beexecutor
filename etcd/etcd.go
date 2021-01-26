package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	bl "github.com/Lz-Gustavo/beemport"
	"github.com/Lz-Gustavo/beemport/pb"

	"go.etcd.io/etcd/clientv3"
)

const (
	defaultEtcdIP   = "127.0.0.1"
	defaultEtcdPort = ":2379"
)

// EtcdClient ...
type EtcdClient struct {
	cl     *clientv3.Client
	cmds   *[]pb.Command
	cancel context.CancelFunc

	lat     bool
	latFile *os.File
}

// NewEtcdClient ...
func NewEtcdClient(ctx context.Context) (*EtcdClient, error) {
	ct, cn := context.WithCancel(ctx)
	ec := &EtcdClient{cancel: cn}

	cl, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdIPAddr + defaultEtcdPort},
		Context:     ct,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	ec.cl = cl

	if latOutFn != "" {
		ec.latFile, err = os.OpenFile(latOutFn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
		ec.lat = true
	}
	return ec, nil
}

func (ec *EtcdClient) sendCommand(ctx context.Context, cmd *pb.Command) error {
	var st time.Time
	if ec.lat {
		st = time.Now()
	}

	var err error
	switch cmd.Op {
	case pb.Command_GET:
		_, err = ec.cl.Get(ctx, cmd.Key)

	case pb.Command_SET:
		_, err = ec.cl.Put(ctx, cmd.Key, cmd.Value)

	case pb.Command_DELETE:
		_, err = ec.cl.Delete(ctx, cmd.Key)

	default:
		return errors.New("unsupported command operation provided")
	}
	if err != nil {
		return err
	}

	if ec.lat {
		dur := time.Since(st)
		if _, err = fmt.Fprintf(ec.latFile, "%d\n", dur); err != nil {
			return err
		}
	}
	return nil
}

func (ec *EtcdClient) loadCommandLog(fn string) error {
	rd, err := os.OpenFile(fn, os.O_RDONLY, 0400)
	if err != nil {
		return err
	}

	cmds, err := bl.UnmarshalLogFromReader(rd)
	if err != nil {
		return err
	}
	ec.cmds = &cmds
	return nil
}

func (ec *EtcdClient) runLoadedLog(ctx context.Context) error {
	if ec.cmds == nil {
		return fmt.Errorf("empty command log, load it first")
	}

	var err error
	for _, cmd := range *ec.cmds {
		err = ec.sendCommand(ctx, &cmd)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ec *EtcdClient) shutdown() {
	ec.cl.Close()
	ec.cancel()
	if ec.lat {
		ec.latFile.Close()
	}
}
