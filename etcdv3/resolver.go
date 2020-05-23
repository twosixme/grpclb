package etcdv3

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"google.golang.org/grpc/resolver"
)

const schema = "etcdv3_resolver"

// Resolver is the implementaion of grpc.resolver
type Resolver struct {
	service   string
	endpoints []string
	client    *clientv3.Client
	cc        resolver.ClientConn
}

// NewResolver return resolver builder
// endpoints example: ["http://127.0.0.1:2379","http://127.0.0.1:12379","http://127.0.0.1:22379"]
func NewResolver(endpoints []string, service string) resolver.Builder {
	return &Resolver{endpoints: endpoints, service: service}
}

// Scheme return etcdv3 schema
func (r *Resolver) Scheme() string {
	return schema
}

// ResolveNow
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
}

// Close
func (r *Resolver) Close() {
}

// Build to resolver.Resolver
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	var err error

	r.client, err = clientv3.New(clientv3.Config{
		Endpoints: r.endpoints,
	})
	if err != nil {
		return nil, fmt.Errorf("grpclb: create clientv3 client failed: %v", err)
	}
	r.cc = cc

	go r.watch(fmt.Sprintf("/%s/%s/", schema, r.service))

	return r, nil
}

func (r *Resolver) watch(prefix string) {
	updates := make(map[string]resolver.Address)

	updateState := func() {
		addresses := make([]resolver.Address, 0, len(updates))
		for _, address := range updates {
			addresses = append(addresses, address)
		}
		state := resolver.State{
			Addresses: addresses,
		}
		r.cc.UpdateState(state)
	}
	resp, err := r.client.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err == nil {
		for _, kv := range resp.Kvs {
			updates[string(kv.Key)] = resolver.Address{Addr: string(kv.Value)}
		}
	}
	updateState()

	rch := r.client.Watch(context.Background(), prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				updates[string(ev.Kv.Key)] = resolver.Address{Addr: string(ev.Kv.Value)}
			case mvccpb.DELETE:
				delete(updates, string(ev.PrevKv.Key))
			}
		}
		updateState()
	}
}
