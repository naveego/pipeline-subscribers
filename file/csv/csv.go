package csv

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/naveego/api/pipeline/subscriber"
	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/api/utils"
)

type Subscriber struct {
	out             *os.File
	shape           pipeline.ShapeDefinition
	columnSeparator string
	quoteCharacter  string
	headersWritten  bool
}

func NewSubscriber() subscriber.Subscriber {
	return &Subscriber{}
}

func (s *Subscriber) Init(ctx subscriber.Context, settings map[string]interface{}) error {
	mr := utils.NewMapReader(settings)

	shapeFile, ok := mr.ReadString("shape_file")
	if !ok {
		return fmt.Errorf("Please provide a shape file")
	}

	outPath, ok := mr.ReadString("out")
	if !ok {
		return fmt.Errorf("Please provide an out put path")
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
		return fmt.Errorf("Could not create output file: %v", err)
	}

	var shape pipeline.ShapeDefinition
	shapeBytes, err := ioutil.ReadFile(shapeFile)
	if err != nil {
		ctx.Logger.Error("Could not read shape file: ", err)
		return fmt.Errorf("Could not read shape file: %v", err)
	}

	err = json.Unmarshal(shapeBytes, &shape)
	if err != nil {
		ctx.Logger.Error("Could not read shape file: ", err)
		return fmt.Errorf("Could not read shape file: %v", err)
	}

	quoteCharacter, _ := mr.ReadString("quote_character")

	columnSeparator, _ := mr.ReadString("column_separator")

	s.columnSeparator = columnSeparator
	s.quoteCharacter = quoteCharacter
	s.shape = shape
	s.out = out

	return nil
}

func (s *Subscriber) TestConnection(ctx subscriber.Context, connSettings map[string]interface{}) (bool, string, error) {
	return true, "", nil
}

func (s *Subscriber) Shapes(ctx subscriber.Context) (pipeline.ShapeDefinitions, error) {
	var shape pipeline.ShapeDefinition
	mr := utils.NewMapReader(ctx.Subscriber.Settings)
	shapeFile, ok := mr.ReadString("shape_file")
	if !ok {
		return pipeline.ShapeDefinitions{}, fmt.Errorf("Please provide a shape file")
	}
	shapeBytes, err := ioutil.ReadFile(shapeFile)
	if err != nil {
		ctx.Logger.Error("Could not read shape file: ", err)
		return pipeline.ShapeDefinitions{}, fmt.Errorf("Could not read shape file: %v", err)
	}

	err = json.Unmarshal(shapeBytes, &shape)
	if err != nil {
		ctx.Logger.Error("Could not read shape file: ", err)
		return pipeline.ShapeDefinitions{}, fmt.Errorf("Could not read shape file: %v", err)
	}
	s.shape = shape
	return pipeline.ShapeDefinitions{s.shape}, nil
}

func (s *Subscriber) Receive(ctx subscriber.Context, shape pipeline.ShapeDefinition, dataPoint pipeline.DataPoint) error {

	if !s.headersWritten {
		headerStr := ""
		for _, m := range ctx.Pipeline.Mappings {
			headerStr = headerStr + m.To + s.columnSeparator
		}

		headerStr = strings.TrimSuffix(headerStr, s.columnSeparator)
		fmt.Fprint(s.out, headerStr+"\r\n")
		s.headersWritten = true
	}

	valStr := ""
	for _, m := range ctx.Pipeline.Mappings {

		v, ok := dataPoint.Data[m.From]
		if ok && v != nil {
			valStr = valStr + fmt.Sprintf("%v", v) + s.columnSeparator
		} else {
			valStr = valStr + s.columnSeparator
		}
	}

	valStr = strings.TrimSuffix(valStr, s.columnSeparator)

	fmt.Fprintf(s.out, valStr+"\r\n")

	return nil
}

func (s *Subscriber) Dispose(ctx subscriber.Context) error {
	if s.out != nil {
		err := s.out.Close()
		s.out = nil
		if err != nil {
			return err
		}
	}

	return nil
}
