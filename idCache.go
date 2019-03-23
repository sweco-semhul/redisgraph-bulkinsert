package main

import (
	"fmt"
	"log"
)

type CacheKey struct {
	label string
	id    string
}

type IdCache struct {
	cache   map[CacheKey]int64
	nextval int64
}

func NewIdCache() IdCache {
	return IdCache{
		cache:   make(map[CacheKey]int64),
		nextval: 0,
	}
}

func (idc IdCache) Get(label string, id string) (int64, error) {
	val, ok := idc.cache[CacheKey{label: label, id: id}]
	if !ok {
		log.Printf("idcache lookup: {%v,%v}=>UNMAPPED, label, id")
		return 0, fmt.Errorf("No node index found for %v:%v", label, id)
	}
	log.Printf("idcache lookup: {%v,%v}=>%v", label, id, val)
	return int64(val), nil
}

func (idc *IdCache) Put(label string, id string) (int64, error) {
	if len(label) == 0 || len(id) == 0 {
		return 0, fmt.Errorf("Null value for label or node id")
	}
	idc.nextval++
	idc.cache[CacheKey{label: label, id: id}] = idc.nextval
	return idc.nextval, nil
}
