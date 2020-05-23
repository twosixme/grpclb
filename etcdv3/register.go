package etcdv3

import (
	"context"
	"fmt"
	"net"

	"github.com/coreos/etcd/clientv3"
)

var deregister = make(chan struct{})

// Register
func Register(endpoints []string, service, host, port string, ttl int64) error {
	serviceValue := net.JoinHostPort(host, port)
	serviceKey := fmt.Sprintf("/%s/%s/%s", schema, service, serviceValue)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
	})
	if err != nil {
		return fmt.Errorf("grpclb: create clientv3 client failed: %v", err)
	}
	resp, err := cli.Grant(context.TODO(), ttl)
	if err != nil {
		return fmt.Errorf("grpclb: create clientv3 lease failed: %v", err)
	}

	if _, err := cli.Put(context.TODO(), serviceKey, serviceValue, clientv3.WithLease(resp.ID)); err != nil {
		return fmt.Errorf("grpclb: set service '%s' with ttl to clientv3 failed: %s", service, err.Error())
	}

	if _, err := cli.KeepAlive(context.TODO(), resp.ID); err != nil {
		return fmt.Errorf("grpclb: refresh service '%s' with ttl to clientv3 failed: %s", service, err.Error())
	}

	// wait deregister then delete
	go func() {
		<-deregister
		_, _ = cli.Delete(context.Background(), serviceKey)
		deregister <- struct{}{}
	}()

	return nil
}

// UnRegister delete registered service from etcd
func UnRegister() {
	deregister <- struct{}{}
	<-deregister
}
