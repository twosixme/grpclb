package balancer

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type HashFunc func(data []byte) uint32

const (
	defaultReplicas = 10
	salt            = "n*@if09g3n"
)

type Uint32Slice []uint32

func (p Uint32Slice) Len() int           { return len(p) }
func (p Uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Uint32Slice) Sort()              { sort.Sort(p) }
func sortUint32s(a []uint32)             { sort.Sort(Uint32Slice(a)) }

func defaultHash(data []byte) uint32 {
	f := fnv.New32()
	_, _ = f.Write(data)
	return f.Sum32()
}

type Ketama struct {
	mu       sync.RWMutex
	hash     HashFunc
	replicas int
	keys     []uint32
	buckets  map[uint32]string
}

func NewKetama(replicas int, hash HashFunc) *Ketama {
	h := &Ketama{
		replicas: replicas,
		hash:     hash,
		buckets:  make(map[uint32]string),
	}
	if h.replicas <= 0 {
		h.replicas = defaultReplicas
	}
	if h.hash == nil {
		h.hash = defaultHash
	}
	return h
}

func (h *Ketama) Add(nodes ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, node := range nodes {
		for i := 0; i < h.replicas; i++ {
			key := h.hash([]byte(salt + strconv.Itoa(i) + node))
			if _, ok := h.buckets[key]; !ok {
				h.keys = append(h.keys, key)
			}
			h.buckets[key] = node
		}
	}
	sortUint32s(h.keys)
}

func (h *Ketama) Remove(nodes ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var deletedKeys []uint32
	for _, node := range nodes {
		for i := 0; i < h.replicas; i++ {
			key := h.hash([]byte(salt + strconv.Itoa(i) + node))
			if _, ok := h.buckets[key]; ok {
				deletedKeys = append(deletedKeys, key)
				delete(h.buckets, key)
			}
		}
	}
	if len(deletedKeys) > 0 {
		h.deleteKeys(deletedKeys)
	}
}

func (h *Ketama) deleteKeys(deletedKeys []uint32) {
	sortUint32s(deletedKeys)

	var index int
	var count int
	for _, key := range deletedKeys {
		for ; index < len(h.keys); index++ {
			h.keys[index-count] = h.keys[index]
			if key == h.keys[index] {
				count++
				index++
				break
			}
		}
	}
	for ; index < len(h.keys); index++ {
		h.keys[index-count] = h.keys[index]
	}

	h.keys = h.keys[:len(h.keys)-count]
}

func (h *Ketama) Get(key string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.keys) == 0 {
		return "", false
	}
	hash := h.hash([]byte(key))

	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})
	if idx == len(h.keys) {
		idx = 0
	}
	value, ok := h.buckets[h.keys[idx]]

	return value, ok
}
