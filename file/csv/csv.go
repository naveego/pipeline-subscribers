package csv

import (
	"fmt"
	"os"

	"github.com/naveego/api/pipeline/subscriber"
	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/api/utils"
)

type Subscriber struct {
	out *os.File
}

func NewSubscriber() subscriber.Subscriber {
	return &Subscriber{}
}

func (s *Subscriber) Init(ctx subscriber.Context, settings map[string]interface{}) error {
	mr := utils.NewMapReader(settings)

	outPath, ok := mr.ReadString("out")
	if !ok {
		return fmt.Errorf("Please provide an out put path")
	}

	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("Could not create output file: %v", err)
	}

	s.out = out
	return nil
}

func (s *Subscriber) TestConnection(ctx subscriber.Context, connSettings map[string]interface{}) (bool, string, error) {
	return true, "", nil
}

func (s *Subscriber) Shapes(ctx subscriber.Context) (pipeline.ShapeDefinitions, error) {
	shapes := pipeline.ShapeDefinitions{}
	shapes = append(shapes, pipeline.ShapeDefinition{
		Name: "person",
		Keys: []string{"id"},
		Properties: []pipeline.PropertyDefinition{
			pipeline.PropertyDefinition{
				Name: "id",
				Type: "string",
			},
			pipeline.PropertyDefinition{
				Name: "name",
				Type: "string",
			},
		},
	})
	return shapes, nil
}

func (s *Subscriber) Receive(ctx subscriber.Context, shape pipeline.ShapeDefinition, dataPoint pipeline.DataPoint) error {
	s.out.WriteString("This worked \r\n")
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
