package balancer

import (
	"math/rand"
	"strconv"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

const (
	Random    = "random"
	WeightKey = "weight"
)

func getWeight(addr resolver.Address) int {
	if addr.Attributes == nil {
		return 1
	}
	value := addr.Attributes.Value(WeightKey)
	if value != nil {
		w, ok := value.(string)
		if ok {
			n, err := strconv.Atoi(w)
			if err == nil && n > 0 {
				return n
			}
		}
	}
	return 1
}

// newRandomBuilder creates a new random balancer builder
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
