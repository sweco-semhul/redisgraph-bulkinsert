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
	"sync"

	"github.com/gomodule/redigo/redis"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: redisgraph_bulkimport <configfile>\n")
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

func process(config Config) {
	impb := NewImportBuffers()

	conn, err := redis.Dial("tcp", config.Redis.Url)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for _, fileConfig := range config.Files {
		impb.SetConfig(fileConfig)
		entityChan := processRecord(readFile(fileConfig), fileConfig)
		BulkInsert(entityChan, fileConfig, config.Redis.GraphName, impb, conn)
	}
}

func BulkInsert(entityChan chan interface{}, fileConf File, graphName string, impb *ImportBuffers, conn redis.Conn) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		var count = 0
		for entity := range entityChan {
			count++
			switch val := entity.(type) {
			case Node:
				impb.AddNode(val)
			case Edge:
				impb.AddEdge(val)
			}
			if count%100000 == 0 {
				log.Printf("count: %v", count)
				impb.PrettyPrint()
				executeQuery("GRAPH.BULK", impb.WriteQuery(graphName), conn)
			}
		}
		impb.PrettyPrint()
		executeQuery("GRAPH.BULK", impb.WriteQuery(graphName), conn)
		wg.Done()
	}()
	wg.Wait()
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
		/*
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
		*/
		wg.Done()
	}()

	wg.Wait()
}

//

func executeQuery(cmd string, query []interface{}, conn redis.Conn) {
	//log.Printf("execute: %v", query)
	r, err := conn.Do(cmd, query...)
	if err != nil {
		log.Fatalf("Error executing [%v]: %v", query, err)
	}
	log.Printf("redis response: %s", r)
	//if err := conn.Flush(); err != nil {
	//	log.Fatal(err)
	//}

	//_, err = redis.Values(r[0], nil)
	//if err != nil {
	//	log.Fatal(err)
	//}
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
				var srcID = record[colIdx[conf.Src.Value]]
				var dstID = record[colIdx[conf.Dst.Value]]

				entityChan <- Edge{
					Label:       label,
					Properties:  mapProperties(conf.Properties, colIdx, record),
					SrcLabel:    conf.Src.Label,
					SrcID:       srcID,
					SrcProperty: map[string]interface{}{conf.Src.Value: srcID},
					DstLabel:    conf.Dst.Label,
					DstID:       dstID,
					DstProperty: map[string]interface{}{conf.Dst.Value: dstID},
				}
			}
		}
		close(entityChan)
	}()
	return entityChan
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
	case "double":
		floatVal, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0.0
			//log.Fatal(err)
		}
		return floatVal
	case "long":
		intVal, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0
			//log.Fatal(err)
		}
		return intVal
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
