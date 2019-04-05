package main

import (
	"bytes"
	"encoding/binary"
	"log"
)

const (
	BI_NULL    uint8 = 0
	BI_BOOL    uint8 = 1
	BI_NUMERIC uint8 = 2
	BI_STRING  uint8 = 3
)

type ImportBuffers struct {
	nodeBuffers  map[string]*bytes.Buffer // buffer for label
	edgeBuffers  map[string]*bytes.Buffer // buffer for label
	counts       map[string]int           // number of entities written
	IDCache      *IDCache                 //
	edgeMappings map[string]EdgeMapping
	nodeMappings map[string]NodeMapping
}

func NewImportBuffers() *ImportBuffers {
	return &ImportBuffers{
		nodeBuffers: make(map[string]*bytes.Buffer),
		edgeBuffers: make(map[string]*bytes.Buffer),
		counts:      make(map[string]int),
		//propertyMappings: make(map[string][]PropertyMapping),
		IDCache:      NewIDCache(),
		edgeMappings: make(map[string]EdgeMapping),
		nodeMappings: make(map[string]NodeMapping),
	}
}

func (ib *ImportBuffers) AddConfigs(fc File) {
	for label, nodeMapping := range fc.Nodes {
		ib.nodeMappings[label] = nodeMapping
	}
	for label, edgeMapping := range fc.Edges {
		ib.edgeMappings[label] = edgeMapping
	}
}

func (ib *ImportBuffers) GetBuffer(val interface{}) *bytes.Buffer {

	switch entity := val.(type) {
	case Node:
		buffer, exists := ib.nodeBuffers[entity.Label]
		if !exists {
			ib.nodeBuffers[entity.Label] = &bytes.Buffer{}
			buffer = ib.nodeBuffers[entity.Label]
		}
		return buffer
	case Edge:
		buffer, exists := ib.edgeBuffers[entity.Label]
		if !exists {
			ib.edgeBuffers[entity.Label] = &bytes.Buffer{}
			buffer = ib.edgeBuffers[entity.Label]
		}
		return buffer
	default:
		log.Printf("GetBuffer for non Node or Edge")
	}

	return nil
}

func (ib *ImportBuffers) GetCount(label string) int {
	count, exists := ib.counts[label]
	if !exists {
		ib.counts[label] = 0
		count = ib.counts[label]
	}
	return count
}

func (ib *ImportBuffers) AddCount(label string) {
	count, exists := ib.counts[label]
	if !exists {
		count = 0
	}
	ib.counts[label] = count + 1
}

func (ib *ImportBuffers) AddNode(node Node) (int, int) {
	label := node.Label
	buffer := ib.GetBuffer(node)
	if ib.GetCount(label) == 0 {
		ib.WriteHeader(buffer, label, ib.nodeMappings[label].Properties)
	}
	ib.WriteProperties(buffer, ib.nodeMappings[label].Properties, node.Properties)
	ib.AddCount(label)
	ib.IDCache.Put(label, node.ID)
	return ib.GetCount(label), buffer.Len()
}

func (ib *ImportBuffers) AddEdge(edge Edge) (int, int) {
	label := edge.Label
	buffer := ib.GetBuffer(edge)
	if ib.GetCount(label) == 0 {
		ib.WriteHeader(buffer, label, ib.edgeMappings[label].Properties)
	}

	// Write SRC + TRG relation

	ib.WriteProperties(buffer, ib.edgeMappings[label].Properties, edge.Properties)
	ib.AddCount(label)
	return ib.GetCount(label), buffer.Len()
}

func (ib *ImportBuffers) WriteProperties(buffer *bytes.Buffer, pms []PropertyMapping, props map[string]interface{}) {
	for _, pm := range pms {
		ib.WriteProperty(buffer, props[pm.ColName])
	}
}

func (ib *ImportBuffers) WriteProperty(buffer *bytes.Buffer, val interface{}) {
	switch value := val.(type) {
	case string:
		binary.Write(buffer, binary.LittleEndian, BI_STRING)
		binary.Write(buffer, binary.LittleEndian, []byte(value))
		binary.Write(buffer, binary.LittleEndian, uint8(0x00))
	case float64, float32:
		binary.Write(buffer, binary.LittleEndian, BI_NUMERIC)
		binary.Write(buffer, binary.LittleEndian, value)
	case bool:
		binary.Write(buffer, binary.LittleEndian, BI_BOOL)
		binary.Write(buffer, binary.LittleEndian, value)
	default:
		log.Fatalf("Unknown value type: %v", value)
	}
}

func (ib *ImportBuffers) WriteHeader(buffer *bytes.Buffer, label string, pms []PropertyMapping) error {

	// label
	binary.Write(buffer, binary.LittleEndian, []byte(string(label)))
	binary.Write(buffer, binary.LittleEndian, uint8(0x00))
	binary.Write(buffer, binary.LittleEndian, uint32(len(pms)))

	// property names
	for _, pm := range pms {
		binary.Write(buffer, binary.LittleEndian, []byte(pm.ColName))
		binary.Write(buffer, binary.LittleEndian, uint8(0x00))
	}
	log.Printf("Header written: [:%v {%v}", label, pms)
	return nil
}

func (ib *ImportBuffers) PrettyPrint() {
	log.Printf("Node Buffer sizes")
	for label, buffer := range ib.nodeBuffers {
		log.Printf("\t%-20v %v", label, buffer.Len())
	}

	log.Printf("Edge Buffer sizes")
	for label, buffer := range ib.edgeBuffers {
		log.Printf("\t%-20v %v", label, buffer.Len())
	}

	log.Printf("Counts")
	for label, count := range ib.counts {
		log.Printf("\t%-20v %v", label, count)
	}

	log.Printf("IDCache entries")
	log.Printf("%v", len(ib.IDCache.cache))
}
