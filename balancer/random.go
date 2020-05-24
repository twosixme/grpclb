package balancer

import (
	"math/rand"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const Random = "random"

func newRandomBuilder() balancer.Builder {
	return base.NewBalancerBuilderV2(Random, &randomPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newRandomBuilder())
}

type randomPickerBuilder struct{}

func (*randomPickerBuilder) Build(info base.PickerBuildInfo) balancer.V2Picker {
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

	return &randomPicker{
		subConns: scs,
	}
}

type randomPicker struct {
	subConns []balancer.SubConn
}

func (p *randomPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	sc := p.subConns[rand.Intn(len(p.subConns))]
	return balancer.PickResult{
		SubConn: sc,
	}, nil
}
