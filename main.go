package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gomodule/redigo/redis"
)

const (
	BI_NULL    uint8 = 0
	BI_BOOL    uint8 = 1
	BI_NUMERIC uint8 = 2
	BI_STRING  uint8 = 3
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

func processNodes(config File, data []string) (int, error) {
	// WriteHeader
	for _, nodeMapping := range config.Nodes {
		buffer, exists := buffers[nodeMapping.Label]
		if !exists {
			buffer = &bytes.Buffer{}
			buffers[nodeMapping.Label] = buffer
			writeHeader(buffer, nodeMapping.Label, nodeMapping.GetPropertyNames())
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
	counters[mapping.Label]++
	return processProperties(mapping.Properties, colMap, data, buf)
}

func processRelations(config File, data []string) (int, error) {
	for _, relMapping := range config.Relations {
		buffer, exists := buffers[relMapping.Label]
		if !exists {
			buffer = &bytes.Buffer{}
			buffers[relMapping.Label] = buffer
			writeHeader(buffer, relMapping.Label, relMapping.GetPropertyNames())
		}
		_, err := processRelation(relMapping, config.ColumNameIndexMap(), data, buffer)
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
	srcId, err := idCache.Get(mapping.Src.Label, data[colMap[mapping.Src.Value]])
	if err != nil {
		return 0, err
	}
	err = binary.Write(buf, binary.LittleEndian, uint64(srcId))
	if err != nil {
		return 0, err
	}
	dstId, err := idCache.Get(mapping.Dst.Label, data[colMap[mapping.Dst.Value]])
	if err != nil {
		return 0, err
	}
	err = binary.Write(buf, binary.LittleEndian, uint64(dstId))
	if err != nil {
		return 0, err
	}
	counters[mapping.Label]++
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
			floatVal, err := strconv.ParseFloat("3.1415", 64)
			if err != nil {
				return 0, err
			}
			err = binary.Write(buf, binary.LittleEndian, BI_NUMERIC)
			if err != nil {
				return 0, err
			}
			err = binary.Write(buf, binary.LittleEndian, floatVal)
			if err != nil {
				return 0, err
			}
		case "bool":
			boolVal, err := strconv.ParseBool(val)
			if err != nil {
				return 0, err
			}
			err = binary.Write(buf, binary.LittleEndian, BI_BOOL)
			if err != nil {
				return 0, err
			}
			err = binary.Write(buf, binary.LittleEndian, boolVal)
			if err != nil {
				return 0, err
			}
		case "string":
			err := binary.Write(buf, binary.LittleEndian, BI_STRING)
			if err != nil {
				return 0, err
			}
			err = binary.Write(buf, binary.LittleEndian, []byte(val))
			if err != nil {
				return 0, err
			}
			err = binary.Write(buf, binary.LittleEndian, uint8(0x00))
			if err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("Unknown datatype: %v", propMap.Type)
		}
	}
	return 1, nil
}

func writeHeader(buf *bytes.Buffer, label string, propNames []string) error {
	var err error
	// label
	err = binary.Write(buf, binary.LittleEndian, []byte(string(label)))
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, uint8(0x00))
	if err != nil {
		return err
	}

	// propertyCount
	err = binary.Write(buf, binary.LittleEndian, uint32(len(propNames)))
	if err != nil {
		return err
	}

	// property names
	for _, propName := range propNames {
		err = binary.Write(buf, binary.LittleEndian, []byte(propName))
		if err != nil {
			return err
		}
		err = binary.Write(buf, binary.LittleEndian, uint8(0x00))
		if err != nil {
			return err
		}
	}
	log.Printf("Header written: [:%v {%v}", label, propNames)
	return nil
}

func processRow(config File, row []string, rowNum int64) error {
	_, err := processNodes(config, row)
	if err != nil {
		return err
	}
	_, err = processRelations(config, row)
	if err != nil {
		return err
	}
	flushBuffers(false, config)
	return nil
}

// flushBuffers flushes buffers
func flushBuffers(force bool, config File) {
	log.Printf("Flushing nodes")
	for _, nm := range config.Nodes {
		count := counters[nm.Label]
		if force || (count > 0 && count%50000 == 0) {
			buffer := buffers[nm.Label]
			log.Printf("Sending nodes buffer: %20v [count: %6v size: %6v]", nm.Label, count, buffer.Len())
			sendNodes(redisConn, buffer.Bytes(), count)

			buffer.Reset()
			counters[nm.Label] = 0
			err := writeHeader(buffer, nm.Label, nm.GetPropertyNames())
			if err != nil {
				log.Fatalf("Error writing header: %v", err)
			}
		}
	}

	log.Printf("Flushing relations")
	for _, rm := range config.Relations {
		count := counters[rm.Label]
		if force || (count > 0 && count%20000 == 0) {
			buffer := buffers[rm.Label]
			log.Printf("Sending relations buffer: %20v [count: %6v size: %6v]", rm.Label, count, buffer.Len())
			sendRelations(redisConn, buffer.Bytes(), count)
			buffer.Reset()
			counters[rm.Label] = 0
			err := writeHeader(buffer, rm.Label, rm.GetPropertyNames())
			if err != nil {
				log.Fatalf("Error writing header: %v", err)
			}
		}
	}

}

func sendRelations(conn redis.Conn, buf []byte, count int) {
	var args = []interface{}{
		"GIM",
	}

	if sendBegin == true {
		args = append(args, []interface{}{"BEGIN", 0, count, 0, 1, buf}...)
		sendBegin = false
	} else {
		args = append(args, []interface{}{0, count, 0, 1, buf}...)
	}

	if err := conn.Send("GRAPH.BULK", args...); err != nil {
		log.Fatal(err)
	}
	if err := conn.Flush(); err != nil {
		log.Fatal(err)
	}
	reply, err := conn.Receive()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%v", reply)

}

func sendNodes(conn redis.Conn, buf []byte, count int) {
	var args = []interface{}{
		"GIM",
	}

	if sendBegin == true {
		args = append(args, []interface{}{"BEGIN", count, 0, 1, 0, buf}...)
		sendBegin = false
	} else {
		args = append(args, []interface{}{count, 0, 1, 0, buf}...)
	}

	if err := conn.Send("GRAPH.BULK", args...); err != nil {
		log.Fatal(err)
	}
	if err := conn.Flush(); err != nil {
		log.Fatal(err)
	}
	reply, err := conn.Receive()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s", reply)

}

func printBuffers() {
	for key, val := range buffers {
		log.Printf("buffer %v: size: %v count: %v", key, val.Cap(), counters[key])
	}
}

func processFile(config File) (int64, error) {
	log.Printf("Processing: %v", config.Filename)
	row := int64(0)
	csvReader, err := getReader(config.Filename, config.Separator)
	if err != nil {
		return row, err
	}

	// If file has Headers, skip first row
	if config.Header {
		_, err := csvReader.Read()
		if err != nil {
			return 0, err
		}
	}

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		err = processRow(config, record, row)
		if err != nil {
			return row, err
		}
		row++

	}
	flushBuffers(true, config)
	log.Printf("Processed: %v", row)
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

var (
	// global node index
	idCache   IdCache                  = NewIdCache()
	buffers   map[string]*bytes.Buffer = make(map[string]*bytes.Buffer)
	counters  map[string]int           = make(map[string]int)
	redisConn redis.Conn               = nil
	sendBegin bool                     = true
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

	redisConn, err = redis.Dial("tcp", conf.Redis.Url)
	if err != nil {
		log.Fatalf("Error connectig to redis: %v", err)
	}

	for _, file := range conf.Files {
		_, err := processFile(file)
		if err != nil {
			log.Fatalf("Unable to process file: %v", err)
		}
	}
	redisConn.Close()
}
