package main

import (
	"fmt"
	"strings"
)

type Node struct {
	ID         string
	Alias      string
	Label      string
	Properties map[string]interface{}
}

func (n Node) Cypher() string {
	var str strings.Builder

	str.WriteString("(")
	str.WriteString(n.Alias)
	str.WriteString(":")
	str.WriteString(n.Label)
	str.WriteString(" ")
	str.WriteString("{")
	p := make([]string, 0, len(n.Properties))

	for k, v := range n.Properties {
		switch val := v.(type) {
		case string:
			p = append(p, fmt.Sprintf("%s:\"%s\"", k, val))
		case float64, float32:
			p = append(p, fmt.Sprintf("%s:%f", k, val))
		default:
			p = append(p, fmt.Sprintf("%s:%v", k, val))
		}
	}

	str.WriteString(strings.Join(p, ", "))
	str.WriteString("}")
	str.WriteString(")")

	return str.String()
}

type Edge struct {
	Label       string
	Properties  map[string]interface{}
	SrcLabel    string
	SrcID       string
	SrcProperty map[string]interface{}
	DstLabel    string
	DstID       string
	DstProperty map[string]interface{}
}

func (e Edge) Cypher() string {
	var str strings.Builder
	src := Node{
		Alias:      "src",
		Label:      e.SrcLabel,
		Properties: e.SrcProperty,
	}
	dst := Node{
		Alias:      "dst",
		Label:      e.DstLabel,
		Properties: e.DstProperty,
	}
	str.WriteString("MATCH ")
	str.WriteString(src.Cypher())
	str.WriteString(",")
	str.WriteString(dst.Cypher())
	str.WriteString(" CREATE ")
	str.WriteString("(src)")
	str.WriteString("-[:")
	str.WriteString(e.Label)
	str.WriteString("]->")
	str.WriteString("(dst)")
	return str.String()
}
