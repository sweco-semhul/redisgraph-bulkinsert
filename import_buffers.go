package main

import (
	"bytes"
	"encoding/binary"
	"log"
)

const (
	BI_NULL   uint8 = 0
	BI_BOOL   uint8 = 1
	BI_DOUBLE uint8 = 2
	BI_STRING uint8 = 3
	BI_LONG   uint8 = 4
)

type ImportBuffers struct {
	first   bool
	nodes   map[string][]Node
	edges   map[string][]Edge
	IDCache *IDCache
	file    File
}

func NewImportBuffers() *ImportBuffers {
	return &ImportBuffers{
		first:   true,
		nodes:   nil,
		edges:   nil,
		IDCache: NewIDCache(),
	}
}

func (ib *ImportBuffers) SetConfig(file File) {
	ib.nodes = make(map[string][]Node)
	ib.edges = make(map[string][]Edge)
	ib.file = file
}

func (ib *ImportBuffers) AddNode(node Node) int {
	_, exists := ib.nodes[node.Label]
	if !exists {
		ib.nodes[node.Label] = []Node{node}
	} else {
		ib.nodes[node.Label] = append(ib.nodes[node.Label], node)
	}

	return len(ib.nodes)
}

func (ib *ImportBuffers) AddEdge(edge Edge) {
	_, exists := ib.edges[edge.Label]
	if !exists {
		ib.edges[edge.Label] = []Edge{edge}
	} else {
		ib.edges[edge.Label] = append(ib.edges[edge.Label], edge)
	}
}

func (ib *ImportBuffers) WriteQuery(graphName string) []interface{} {
	// iterate nodes and edges, create a buffer

	var buffer = &bytes.Buffer{}

	var nodeCount = 0
	var nodePackages = len(ib.nodes)
	var nodeMap = ib.nodes
	ib.nodes = make(map[string][]Node)
	for label, nodes := range nodeMap {
		var propertyMapping = ib.file.Nodes[label].Properties
		ib.writeHeader(buffer, label, propertyMapping)
		for _, node := range nodes {
			ib.writeProperties(buffer, propertyMapping, node.Properties)
			ib.IDCache.Put(label, node.ID)
			nodeCount++
		}
	}

	var edgeCount = 0
	var edgePackages = len(ib.edges)
	var edgeMap = ib.edges
	ib.edges = make(map[string][]Edge)
	for label, edges := range edgeMap {
		var mapping = ib.file.Edges[label]
		ib.writeHeader(buffer, label, mapping.Properties)
		for _, edge := range edges {
			err := ib.writeRelation(buffer, edge, mapping)
			if err != nil {
				log.Printf("No nodes yet for: [:%v {%v}", label, edge)
				ib.AddEdge(edge)
			} else {
				edgeCount++
			}
		}
	}

	if ib.first {
		ib.first = false
		return []interface{}{graphName, "BEGIN", nodeCount, edgeCount, nodePackages, edgePackages, buffer}
	}

	return []interface{}{graphName, nodeCount, edgeCount, nodePackages, edgePackages, buffer}
}

func (ib *ImportBuffers) writeRelation(buffer *bytes.Buffer, edge Edge, mapping EdgeMapping) error {
	srcID, err := ib.IDCache.Get(edge.SrcLabel, edge.SrcID)
	if err != nil {
		return err
	}
	dstID, err := ib.IDCache.Get(edge.DstLabel, edge.DstID)
	if err != nil {
		return err
	}

	err = binary.Write(buffer, binary.LittleEndian, srcID)
	if err != nil {
		return err
	}
	err = binary.Write(buffer, binary.LittleEndian, dstID)
	if err != nil {
		return err
	}

	ib.writeProperties(buffer, mapping.Properties, edge.Properties)

	return nil
}

func (ib *ImportBuffers) writeProperties(buffer *bytes.Buffer, pms []PropertyMapping, props map[string]interface{}) {
	for _, pm := range pms {
		ib.writeProperty(buffer, props[pm.ColName])
	}
}

func (ib *ImportBuffers) writeProperty(buffer *bytes.Buffer, val interface{}) {
	switch value := val.(type) {
	case string:
		binary.Write(buffer, binary.LittleEndian, BI_STRING)
		binary.Write(buffer, binary.LittleEndian, []byte(value))
		binary.Write(buffer, binary.LittleEndian, uint8(0x00))
	case float64, float32:
		binary.Write(buffer, binary.LittleEndian, BI_DOUBLE)
		binary.Write(buffer, binary.LittleEndian, value)
	case int64, int32:
		binary.Write(buffer, binary.LittleEndian, BI_LONG)
		binary.Write(buffer, binary.LittleEndian, value)
	case bool:
		binary.Write(buffer, binary.LittleEndian, BI_BOOL)
		binary.Write(buffer, binary.LittleEndian, value)
	default:
		log.Fatalf("Unknown value type: %v", value)
	}
}

func (ib *ImportBuffers) writeHeader(buffer *bytes.Buffer, label string, pms []PropertyMapping) error {

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
	log.Printf("Node sizes")
	for label, nodes := range ib.nodes {
		log.Printf("\t%-20v %v", label, len(nodes))
	}

	log.Printf("Edge sizes")
	for label, edges := range ib.edges {
		log.Printf("\t%-20v %v", label, len(edges))
	}

	log.Printf("IDCache entries")
	log.Printf("%v", len(ib.IDCache.cache))
}
