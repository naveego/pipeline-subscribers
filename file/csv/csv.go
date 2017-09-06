package csv

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/naveego/api/pipeline/subscriber"
	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/api/utils"
)

type Subscriber struct {
	out             *os.File
	shape           pipeline.ShapeDefinition
	rowSeparator    string
	columnSeparator string
	quoteCharacter  string
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

	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("Could not create output file: %v", err)
	}

	var shape pipeline.ShapeDefinition
	shapeBytes, err := ioutil.ReadFile(shapeFile)
	if err != nil {
		return fmt.Errorf("Could not read shape file: %v", err)
	}

	err = json.Unmarshal(shapeBytes, &shape)
	if err != nil {
		return fmt.Errorf("Could not read shape file: %v", err)
	}

	s.shape = shape
	s.out = out
	return nil
}

func (s *Subscriber) TestConnection(ctx subscriber.Context, connSettings map[string]interface{}) (bool, string, error) {
	return true, "", nil
}

func (s *Subscriber) Shapes(ctx subscriber.Context) (pipeline.ShapeDefinitions, error) {
	return pipeline.ShapeDefinitions{s.shape}, nil
}

func (s *Subscriber) Receive(ctx subscriber.Context, shape pipeline.ShapeDefinition, dataPoint pipeline.DataPoint) error {
	str := ""

	for _, m := range ctx.Pipeline.Mappings {
		valStr := ""

		if v, ok := dataPoint.Data[m.From]; ok {
			valStr = valStr + fmt.Sprintf("%v", v) + s.columnSeparator
		}
	}

	fmt.Fprintf(s.out, str+s.rowSeparator)

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
