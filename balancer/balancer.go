package balancer

import (
	"strconv"

	"google.golang.org/grpc/resolver"
)

const WeightKey = "weight"

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
