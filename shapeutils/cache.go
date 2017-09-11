package shapeutils

import (
	"github.com/naveego/api/types/pipeline"
)

type ShapeCache struct {
	shapes map[string]*KnownShape
}

func NewShapeCache() ShapeCache {
	return ShapeCache{
		shapes: map[string]*KnownShape{},
	}
}

//NewShapeCacheWithShapes creates a new ShapeCache and initializes
// it with the provided shapes.
func NewShapeCacheWithShapes(initialShapes map[string]*KnownShape) ShapeCache {
	c := NewShapeCache()

	for k, v := range initialShapes {
		c.shapes[k] = v
	}

	return c
}

// GetKnownShape returns the KnownShape for a datapoint if it's GetKnownShaped.
// Otherwise it returns an empty KnownShape and GetKnownShaped == false.
func (s *ShapeCache) GetKnownShape(datapoint pipeline.DataPoint) (shape *KnownShape, GetKnownShaped bool) {

	name := canonicalName(datapoint)
	shape, ok := s.shapes[name]

	if !ok || !shape.MatchesShape(datapoint.Shape) {
		return nil, false
	}

	return shape, true
}

// Analyze takes a datapoint and returns the changes that must be applied to the
// data store to accomodate the shape of the datapoint. After the data store
// has been updated, the delta should be passed back to this cache via ApplyDelta
// to update the cached representation of the data store.
func (s *ShapeCache) Analyze(datapoint pipeline.DataPoint) (delta ShapeDelta) {

	newShape := NewKnownShape(datapoint)

	oldShape := s.shapes[newShape.Name]

	return GenerateShapeDelta(oldShape, *newShape)
}

// ApplyDelta updates the cache with the data in a using a ShapeDelta
// and returns the shape which was updated.
func (s *ShapeCache) ApplyDelta(delta ShapeDelta) (shape *KnownShape) {

	newShape := &delta.NewShape
	oldShape, ok := s.shapes[newShape.Name]

	if ok {
		oldShape.Merge(newShape)
		return oldShape
	}

	s.shapes[newShape.Name] = newShape

	return newShape
}

// GetAllShapeDefinitions returns the ShapeDefinitions of all KnownShapes
func (s *ShapeCache) GetAllShapeDefinitions() (shapes []pipeline.ShapeDefinition) {

	for _, x := range s.shapes {
		shapes = append(shapes, x.ShapeDefinition)
	}

	return
}
