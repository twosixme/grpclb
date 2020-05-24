package balancer

import (
	"math/rand"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const RoundRobin = "round_robin"

func newRoundRobinBuilder() balancer.Builder {
	return base.NewBalancerBuilderV2(Random, &roundRobinPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newRoundRobinBuilder())
}

type roundRobinPickerBuilder struct{}

func (*roundRobinPickerBuilder) Build(info base.PickerBuildInfo) balancer.V2Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPickerV2(balancer.ErrNoSubConnAvailable)
	}

	var scs []balancer.SubConn
	for sc, scInfo := range info.ReadySCs {
		weight := getWeight(scInfo.Address)
		for i := 0; i < weight; i++ {
			scs = append(scs, sc)
		}
	}

	return &roundRobinPicker{
		subConns: scs,
		next:     rand.Intn(len(scs)),
	}
}

type roundRobinPicker struct {
	subConns []balancer.SubConn
	next     int
}

func (p *roundRobinPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	sc := p.subConns[p.next]
	p.next = (p.next + 1) % len(p.subConns)
	return balancer.PickResult{
		SubConn: sc,
	}, nil
}
