package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/gomodule/redigo/redis"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: redis_import2pg <configfile>\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}

	conf, err := NewConfig(os.Args[1])
	if err != nil {
		log.Fatalf("Error reading configuration: %v", err)
	}

	process(conf)

}

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
	SrcProperty map[string]interface{}
	DstLabel    string
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

func process(config Config) {
	impb := NewImportBuffers()

	for _, fileConfig := range config.Files {
		impb.AddConfigs(fileConfig)
		entityChan := processRecord(readFile(fileConfig), fileConfig)
		//writeData(entityChan, config.Redis, fileConfig)
		BulkInsert(entityChan, config.Redis, fileConfig, impb)
	}
}

func BulkInsert(entityChan chan interface{}, redisConf Redis, fileConf File, impb *ImportBuffers) {
	conn, err := redis.Dial("tcp", redisConf.Url)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	executeQuery("GRAPH.QUERY", redisConf.GraphName, "CREATE (:Import{id: 1})", conn)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for entity := range entityChan {
			var count, size int
			switch val := entity.(type) {
			case Node:
				count, size = impb.AddNode(val)
			case Edge:
				count, size = impb.AddEdge(val)
			}
			if count%10000 == 0 {
				log.Printf("count: %v  size: %v", count, size)
				impb.PrettyPrint()
			}
		}
		impb.PrettyPrint()
		wg.Done()
	}()
	wg.Wait()
}

func writeNode() {

}

func writeEdge() {

}

func MergeInsert(entityChan chan interface{}, redisConf Redis, fileConf File) {

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		conn, err := redis.Dial("tcp", redisConf.Url)
		defer conn.Close()
		if err != nil {
			log.Fatal(err)
		}

		// Create import object (which also creates the graph)
		executeQuery("GRAPH.QUERY", redisConf.GraphName, "CREATE (:Import {start: '', end: ''})", conn)
		for label, nodeConf := range fileConf.Nodes {
			query := fmt.Sprintf("CREATE INDEX ON :%v(%v)", label, nodeConf.Properties[0].ColName)
			executeQuery("GRAPH.QUERY", redisConf.GraphName, query, conn)
		}

		for entity := range entityChan {
			switch v := entity.(type) {
			case Node:
				query := fmt.Sprintf("MERGE %v", v.Cypher())
				executeQuery("GRAPH.QUERY", redisConf.GraphName, query, conn)
			case Edge:
				query := fmt.Sprintf("%v", v.Cypher())
				executeQuery("GRAPH.QUERY", redisConf.GraphName, query, conn)
			}
		}
		wg.Done()
	}()

	wg.Wait()
}

//

func executeQuery(cmd string, graphname string, query string, conn redis.Conn) {
	//log.Printf("execute: %v", query)
	r, err := redis.Values(conn.Do(cmd, graphname, query))
	if err != nil {
		log.Fatalf("Error executing [%v]: %v", query, err)
	}
	_, err = redis.Values(r[0], nil)
	if err != nil {
		log.Fatal(err)
	}
	//log.Printf("result: %v", results)
}

func readFile(config File) chan []string {
	out := make(chan []string)
	//defer close(out)
	go func() {

		log.Printf("Processing: %v", config.Filename)

		csvReader, err := getReader(config.Filename, config.Separator)
		if err != nil {
			log.Fatal(err)
		}
		if config.Header {
			_, err := csvReader.Read()
			if err != nil {
				log.Fatal(err)
			}
		}
		row := 0
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			out <- record
			row++
		}
		//log.Printf("Processed: %v", config.Filename)
		close(out)
	}()
	return out
}

func getRowCount(filename string) (int, error) {
	infile, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	// Uncompress file based on extension
	var reader io.Reader
	if filepath.Ext(filename) == ".gz" {
		reader, err = gzip.NewReader(infile)
		if err != nil {
			return 0, err
		}
	} else {
		reader = infile
	}

	scanner := bufio.NewScanner(reader)
	rows := 0
	for scanner.Scan() {
		rows++
	}
	infile.Close()
	return rows, nil
}

func getReader(filename string, separator string) (*csv.Reader, error) {
	infile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	// Uncompress file based on extension
	var reader io.Reader
	if filepath.Ext(filename) == ".gz" {
		reader, err = gzip.NewReader(infile)
		if err != nil {
			return nil, err
		}
	} else {
		reader = infile
	}

	csvReader := csv.NewReader(reader)
	// TODO: set from file metadata
	csvReader.Comment = '#'
	csvReader.Comma = '\t' //([]rune(separator))[0]

	return csvReader, nil
}

func processRecord(recordChan chan []string, config File) chan interface{} {
	entityChan := make(chan interface{})

	colIdx := config.ColumNameIndexMap()
	go func() {
		for record := range recordChan {
			for label, conf := range config.Nodes {
				entityChan <- Node{
					ID:         getIDProperty(conf.Properties, colIdx, record),
					Label:      label,
					Properties: mapProperties(conf.Properties, colIdx, record),
				}
			}
			for label, conf := range config.Edges {
				entityChan <- Edge{
					Label:       label,
					Properties:  mapProperties(conf.Properties, colIdx, record),
					SrcLabel:    conf.Src.Label,
					SrcProperty: getReferenceProperty(conf.Src, colIdx, record),
					DstLabel:    conf.Dst.Label,
					DstProperty: getReferenceProperty(conf.Dst, colIdx, record),
				}
			}
		}
		close(entityChan)
	}()
	return entityChan
}

func getReferenceProperty(er EntityReference, colIdx map[string]int, record []string) map[string]interface{} {
	val := record[colIdx[er.Value]]

	return map[string]interface{}{
		er.Value: val,
	}
}

func getIDProperty(props []PropertyMapping, colIdx map[string]int, record []string) string {
	return record[colIdx[props[0].ColName]]
}

func mapProperties(props []PropertyMapping, colIdx map[string]int, record []string) map[string]interface{} {
	result := make(map[string]interface{})
	for _, pm := range props {
		result[pm.ColName] = getValue(pm, record, colIdx)
	}
	return result
}

func getValue(pm PropertyMapping, record []string, colIdx map[string]int) interface{} {

	value := record[colIdx[pm.ColName]]
	// TODO: refactor to be able to use custom converter from
	// property mappping
	return convertValue(value, pm.Type)
}

func convertValue(value string, valueType string) interface{} {
	switch valueType {
	case "numeric":
		floatVal, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.Fatal(err)
		}
		return floatVal
	case "bool":
		boolVal, err := strconv.ParseBool(value)
		if err != nil {
			log.Fatal(err)
		}
		return boolVal
	case "string":
		return value
	}
	return nil
}
