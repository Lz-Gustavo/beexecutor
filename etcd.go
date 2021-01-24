package main

import (
	"context"
	"fmt"
	"os"
	"time"

	bl "github.com/Lz-Gustavo/beelog"
	"github.com/Lz-Gustavo/beelog/pb"
	"go.etcd.io/etcd/clientv3"
)

const (
	etcdURL = "http://127.0.0.1:2379"
)

// EtcdClient ...
type EtcdClient struct {
	cl     *clientv3.Client
	cmds   *[]pb.Command
	cancel context.CancelFunc
}

// NewEtcdClient ...
func NewEtcdClient() (*EtcdClient, error) {
	cl, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdURL},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	ctx, cn := context.WithCancel(context.Background())
	ec := &EtcdClient{
		cl:     cl,
		cancel: cn,
	}

	go ec.runLoadedLog(ctx)
	return ec, nil
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

	// TODO: ...
	return nil
}

func (ec *EtcdClient) shutdown() {
	ec.cancel()
}
