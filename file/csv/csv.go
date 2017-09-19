package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/api/utils"
	"github.com/naveego/navigator-go/subscribers/protocol"
)

type csvSubscriber struct {
	out             *os.File
	shape           pipeline.ShapeDefinition
	columnSeparator string
	quoteCharacter  string
	headersWritten  bool
	mappings        []pipeline.ShapeMapping
}

func (s *csvSubscriber) Init(request protocol.InitRequest) (protocol.InitResponse, error) {
	resp := protocol.InitResponse{}

	mr := utils.NewMapReader(request.Settings)

	shapeFile, ok := mr.ReadString("shape_file")
	if !ok {
		return resp, fmt.Errorf("Please provide a shape file")
	}

	outPath, ok := mr.ReadString("out")
	if !ok {
		return resp, fmt.Errorf("Please provide an out put path")
	}

	appendDateToFile, _ := mr.ReadBool("append_date_to_name")
	if appendDateToFile {
		outExt := filepath.Ext(outPath)
		outBase := outPath
		lastDot := strings.LastIndexByte(outPath, '.')
		if lastDot >= 0 {
			outBase = outPath[:lastDot]
		}

		dateStr := time.Now().Format("200601021504")

		outPath = outBase + dateStr + outExt
	}

	out, err := os.Create(outPath)
	if err != nil {
		return resp, fmt.Errorf("Could not create output file: %v", err)
	}

	var shape pipeline.ShapeDefinition
	shapeBytes, err := ioutil.ReadFile(shapeFile)
	if err != nil {
		return resp, fmt.Errorf("Could not read shape file: %v", err)
	}

	err = json.Unmarshal(shapeBytes, &shape)
	if err != nil {
		return resp, fmt.Errorf("Could not read shape file: %v", err)
	}

	quoteCharacter, _ := mr.ReadString("quote_character")

	columnSeparator, _ := mr.ReadString("column_separator")

	s.columnSeparator = columnSeparator
	s.quoteCharacter = quoteCharacter
	s.shape = shape
	s.out = out
	s.mappings = request.Mappings

	return resp, nil
}

func (s *csvSubscriber) TestConnection(request protocol.TestConnectionRequest) (protocol.TestConnectionResponse, error) {
	return protocol.TestConnectionResponse{Success: true}, nil
}

func (s *csvSubscriber) DiscoverShapes(request protocol.DiscoverShapesRequest) (protocol.DiscoverShapesResponse, error) {
	resp := protocol.DiscoverShapesResponse{}
	var shape pipeline.ShapeDefinition

	mr := utils.NewMapReader(request.Settings)
	shapeFile, ok := mr.ReadString("shape_file")
	if !ok {
		return resp, fmt.Errorf("Please provide a shape file")
	}
	shapeBytes, err := ioutil.ReadFile(shapeFile)
	if err != nil {
		return resp, fmt.Errorf("Could not read shape file: %v", err)
	}

	err = json.Unmarshal(shapeBytes, &shape)
	if err != nil {
		return resp, fmt.Errorf("Could not read shape file: %v", err)
	}
	s.shape = shape
	resp.Shapes = pipeline.ShapeDefinitions{s.shape}
	return resp, nil
}

func (s *csvSubscriber) ReceiveDataPoint(request protocol.ReceiveShapeRequest) (protocol.ReceiveShapeResponse, error) {

	if !s.headersWritten {
		headerStr := ""
		for _, m := range s.mappings {
			headerStr = headerStr + m.To + s.columnSeparator
		}

		headerStr = strings.TrimSuffix(headerStr, s.columnSeparator)
		fmt.Fprint(s.out, headerStr+"\r\n")
		s.headersWritten = true
	}

	valStr := ""
	for _, m := range s.mappings {

		v, ok := request.DataPoint.Data[m.From]
		if ok && v != nil {
			valStr = valStr + fmt.Sprintf("%v", v) + s.columnSeparator
		} else {
			valStr = valStr + s.columnSeparator
		}
	}

	valStr = strings.TrimSuffix(valStr, s.columnSeparator)

	fmt.Fprintf(s.out, valStr+"\r\n")

	return protocol.ReceiveShapeResponse{Success: true}, nil
}

func (s *csvSubscriber) Dispose(request protocol.DisposeRequest) (protocol.DisposeResponse, error) {

	if s.out != nil {
		err := s.out.Close()
		s.out = nil
		if err != nil {
			return protocol.DisposeResponse{}, err
		}
	}

	return protocol.DisposeResponse{}, nil
}
