package main

import (
	"fmt"
	"log"
)

type CacheKey struct {
	label string
	id    string
}

type IDCache struct {
	cache   map[CacheKey]uint64
	nextval uint64
}

func NewIDCache() *IDCache {
	return &IDCache{
		cache:   make(map[CacheKey]uint64),
		nextval: 0,
	}
}

func (idc IDCache) Get(label string, id string) (uint64, error) {
	val, ok := idc.cache[CacheKey{label: label, id: id}]
	if !ok {
		log.Printf("idcache lookup: {%v,%v}=>UNMAPPED", label, id)
		return 0, fmt.Errorf("No node index found for %v:%v", label, id)
	}
	log.Printf("idcache lookup: {%v,%v}=>%v", label, id, val)
	return val, nil
}

func (idc *IDCache) Put(label string, id string) (uint64, error) {
	if len(label) == 0 || len(id) == 0 {
		return 0, fmt.Errorf("Null value for label or node id")
	}
	idc.cache[CacheKey{label: label, id: id}] = idc.nextval
	idc.nextval++
	return idc.nextval, nil
}
