package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

type keyRingEntity struct {
	Addr string
	Hash string
}

type sortBySha256 []*keyRingEntity

func (a sortBySha256) Len() int      { return len(a) }
func (a sortBySha256) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortBySha256) Less(i, j int) bool {
	return a[i].Hash < a[j].Hash
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	entities := make([]*keyRingEntity, 0)
	for k, v := range c.ServerMap {
		entities = append(entities, &keyRingEntity{Addr: k, Hash: v})
	}
	sort.Sort(sortBySha256(entities))
	for _, v := range entities {
		if v.Hash >= blockId {
			return v.Addr
		}
	}
	return entities[0].Addr
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	c := &ConsistentHashRing{ServerMap: make(map[string]string)}
	for _, addr := range serverAddrs {
		c.ServerMap[addr] = c.Hash("blockstore" + addr)
	}
	return c
}
