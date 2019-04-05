package main

import (
	"errors"
	"fmt"
	"os"

	yaml "gopkg.in/yaml.v2"
)

/*

 */

type Config struct {
	Redis Redis
	Files []File
}

type Redis struct {
	Url       string
	Username  string
	Password  string
	GraphName string
}

type File struct {
	Filename  string
	Separator string
	Header    bool
	Columns   []string
	Nodes     map[string]NodeMapping
	Edges     map[string]EdgeMapping
}

func (f File) ColumNameIndexMap() map[string]int {
	result := make(map[string]int)
	for idx, col := range f.Columns {
		result[col] = idx
	}
	return result
}

type NodeMapping struct {
	//Label      string
	Filters    []string
	Properties []PropertyMapping
}

func (nm NodeMapping) GetPropertyNames() []string {
	var result []string
	for _, val := range nm.Properties {
		result = append(result, val.ColName)
	}
	return result
}

type EdgeMapping struct {
	//Label      string
	Filters    []string
	Src        EntityReference
	Dst        EntityReference
	Properties []PropertyMapping
}

func (rm EdgeMapping) GetPropertyNames() []string {
	var result []string
	for _, val := range rm.Properties {
		result = append(result, val.ColName)
	}
	return result
}

type EntityReference struct {
	Label string
	Value string
}

type PropertyMapping struct {
	ColName   string
	Type      string
	Converter string
}

func NewConfig(fileName string) (Config, error) {
	var config Config
	configFile, err := os.Open(fileName)
	if err != nil {
		return config, err
	}
	defer configFile.Close()
	decoder := yaml.NewDecoder(configFile)
	err = decoder.Decode(&config)
	if err != nil {
		return config, err
	}
	return config, validate(config)
}

func validate(config Config) error {
	if len(config.Files) == 0 {
		return fmt.Errorf("No files specified")
	}
	for _, file := range config.Files {
		err := validateFile(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateFile(file File) error {

	if len(file.Filename) == 0 {
		return fmt.Errorf("Filename not specified")
	}
	if len(file.Separator) == 0 {
		return fmt.Errorf("Separator not specified")
	}

	// Check fileExistance
	if _, err := os.Stat(file.Filename); err != nil {
		if os.IsNotExist(err) {
			return errors.New(fmt.Sprintf("File does not exist: %v", file.Filename))
		}
	}
	return nil
}

/*
TODO: Validations:
	- validate properties:
		- validate column count
		- validate datatype
	validate filters:
		validate operator
*/
