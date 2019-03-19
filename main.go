package main

import (
	"bytes"
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
)

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
	csvReader.Comma = ([]rune(separator))[0]

	return csvReader, nil
}

func filtersMatch(filters []string, colMap map[string]int, data []string) (bool, error) {
	for _, filter := range filters {
		match, err := filterMatch(filter, colMap, data)
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}

func filterMatch(filter string, colMap map[string]int, data []string) (bool, error) {
	parts := strings.Split(filter, " ")
	switch parts[1] {
	case "==":
		return data[colMap[parts[0]]] == parts[2], nil
	case "!=":
		return data[colMap[parts[0]]] != parts[2], nil
	case ">":
		return data[colMap[parts[0]]] > parts[2], nil
	case "<":
		return data[colMap[parts[0]]] < parts[2], nil
	default:
		return false, fmt.Errorf("Unknown operator: %v", parts[1])
	}
}

func processNodes(config File, data []string, buffers map[string]*bytes.Buffer) (int, error) {
	for _, nodeMapping := range config.Nodes {
		buffer, exists := buffers[nodeMapping.Label]
		if !exists {
			buffer = &bytes.Buffer{}
			buffers[nodeMapping.Label] = buffer
		}
		_, err := processNode(nodeMapping, config.ColumNameIndexMap(), data, buffer)
		if err != nil {
			return 0, err
		}
	}
	return 0, nil
}

func processNode(mapping NodeMapping, colMap map[string]int, data []string, buf *bytes.Buffer) (int, error) {
	// evalute filters
	match, err := filtersMatch(mapping.Filters, colMap, data)
	if err != nil {
		return 0, err
	}
	if !match {
		return 0, nil
	}
	// Use first column as id when caching
	idCache.Put(mapping.Label, data[colMap[mapping.Properties[0].ColName]])

	return processProperties(mapping.Properties, colMap, data, buf)
}

func processRelations(config File, data []string, buffers map[string]*bytes.Buffer) (int, error) {
	for _, nodeMapping := range config.Relations {
		buffer, exists := buffers[nodeMapping.Label]
		if !exists {
			buffer = &bytes.Buffer{}
			buffers[nodeMapping.Label] = buffer
		}
		_, err := processRelation(nodeMapping, config.ColumNameIndexMap(), data, buffer)
		if err != nil {
			return 0, err
		}
	}
	return 0, nil
}

func processRelation(mapping RelationMapping, colMap map[string]int, data []string, buf *bytes.Buffer) (int, error) {
	match, err := filtersMatch(mapping.Filters, colMap, data)
	if err != nil {
		return 0, err
	}
	if !match {
		return 0, nil
	}

	/*
		srcId, err := idCache.Get(mapping.Src.Label, mapping.Src.Value)
		dstId, err := idCache.Get(mapping.Dst.Label, mapping.Dst.Value)
	*/
	return processProperties(mapping.Properties, colMap, data, buf)
}

func processProperties(props []PropertyMapping, colMap map[string]int, data []string, buf *bytes.Buffer) (int, error) {
	if len(data) < len(props) {
		return 0, fmt.Errorf("Data contains fewer columns that mapping.")
	}
	for _, propMap := range props {
		val := data[colMap[propMap.ColName]]
		switch propMap.Type {
		case "numeric":
			// Convert to float64
			// writeType(numeric)
			//log.Printf("%v", val)
		case "bool":
			// Convert to float64
			// log.Printf("%v", val)
		case "string":
			buf.Write([]byte(val))
			buf.Write([]byte{0x00})
		default:
			return 0, fmt.Errorf("Unknown datatype: %v", propMap.Type)
		}
	}
	return 1, nil
}

func processRow(config File, row []string, buffers map[string]*bytes.Buffer) error {
	_, err := processNodes(config, row, buffers)
	if err != nil {
		return err
	}
	_, err = processRelations(config, row, buffers)
	if err != nil {
		return err
	}
	return nil
}

func printBuffers(buffers map[string]*bytes.Buffer) {
	for key, val := range buffers {
		log.Printf("buffer %v has size %v", key, val.Cap())
	}
}

var (
	// global node index
	idCache IdCache = NewIdCache()
	//buffers  map[string]*bytes.Buffer = make(map[string]*bytes.Buffer)
	counters map[string]int = make(map[string]int)
)

func processFile(config File) (int64, error) {
	log.Printf("Processing: %v", config.Filename)
	rows := int64(0)
	csvReader, err := getReader(config.Filename, config.Separator)
	if err != nil {
		return rows, err
	}

	buffers := make(map[string]*bytes.Buffer)

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		err = processRow(config, record, buffers)
		if err != nil {
			return rows, err
		}
		rows++

	}
	printBuffers(buffers)
	for key, val := range idCache.cache {
		log.Printf("key: %v val: %v", key, val)
	}
	return 0, nil
}

func EnvString(key string, val string) string {
	if len(os.Getenv(key)) > 0 {
		return os.Getenv(key)
	}
	return val
}

func EnvInt(key string, val int) int {
	var retval int
	var err error
	if len(os.Getenv(key)) > 0 {
		retval, err = strconv.Atoi(os.Getenv(key))
		if err != nil {
			retval = val
		}
	} else {
		retval = val
	}
	return retval
}

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

	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)

	conf, err := NewConfig(os.Args[1])
	if err != nil {
		log.Fatalf("Error reading configuration: %v", err)
	}

	/*
		conn, err := redis.Dial("tcp", conf.Redis.Url)
		if err != nil {
			log.Fatalf("Error connectig to redis: %v", err)
		}
		conn.Close()
	*/

	for _, file := range conf.Files {
		_, err := processFile(file)
		if err != nil {
			log.Fatalf("Unable to process file: %v", err)
		}

	}
}
