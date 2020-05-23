package balancer

import (
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const ConsistentHash = "consistent_hash"
const DefaultConsistentHashKey = "consistent-hash"

func init() {
	balancer.Register(newConsistentHashBuilder(DefaultConsistentHashKey))
}

func InitConsistentHashBuilder(consistanceHashKey string) {
	balancer.Register(newConsistentHashBuilder(consistanceHashKey))
}

// newConsistanceHashBuilder creates a new ConsistanceHash balancer builder.
func newConsistentHashBuilder(consistentHashKey string) balancer.Builder {
	return base.NewBalancerBuilderV2(ConsistentHash, &consistentHashPickerBuilder{consistentHashKey}, base.Config{HealthCheck: true})
}

type consistentHashPickerBuilder struct {
	consistentHashKey string
}

func (b *consistentHashPickerBuilder) Build(info base.PickerBuildInfo) balancer.V2Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPickerV2(balancer.ErrNoSubConnAvailable)
	}

	picker := &consistentHashPicker{
		subConns:          make(map[string]balancer.SubConn),
		hash:              NewKetama(10, nil),
		consistentHashKey: b.consistentHashKey,
	}

	for sc, scInfo := range info.ReadySCs {
		weight := getWeight(scInfo.Address)
		for i := 0; i < weight; i++ {
			addr := fmt.Sprintf("%s-%d", scInfo.Address.Addr, i)
			picker.hash.Add(addr)
			picker.subConns[addr] = sc
		}
	}

	return picker
}

type consistentHashPicker struct {
	subConns          map[string]balancer.SubConn
	hash              *Ketama
	consistentHashKey string
}

func (p *consistentHashPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var sc balancer.SubConn
	value := info.Ctx.Value(p.consistentHashKey)
	if value != nil {
		key, ok := value.(string)
		if ok {
			addr, ok := p.hash.Get(key)
			if ok {
				sc = p.subConns[addr]
			}
		}
	}
	return balancer.PickResult{
		SubConn: sc,
	}, nil
}
