package shapeutils

import (
	"sort"

	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/api/utils"
)

// PropertiesAndTypes is a map that contains the property name
// and the type.  The key will be the name of the property and
// the value will contain the type
type PropertiesAndTypes map[string]string

// ShapeDelta will contain information about the current data points
// shape, with respect to the pipeline shape for the same entity.
// This information can be used by the subcriber to alter its
// storage if necessary.
type ShapeDelta struct {
	IsNew            bool
	HasKeyChanges    bool
	HasNewProperties bool
	//PreviousShapeDef pipeline.ShapeDefinition
	//ShapeDef         pipeline.ShapeDefinition
	Name          string
	NewKeys       []string
	ExistingKeys  []string
	NewProperties PropertiesAndTypes
}

type knownHashes map[uint32]bool

func (k knownHashes) merge(other knownHashes) {
	for h, v := range other {
		k[h] = v
	}
}

type KnownShape struct {
	pipeline.ShapeDefinition

	cache map[string]interface{}

	keyHashes  knownHashes
	propHashes knownHashes
}

// Set caches the value under key. The cache will be wiped if another shape is merged in.
func (k *KnownShape) Set(key string, value interface{}) { k.cache[key] = value }

// Get returns the value cached under key.
func (k *KnownShape) Get(key string) (interface{}, bool) {
	v, ok := k.cache[key]
	return v, ok
}

func (k *KnownShape) MatchesShape(shape pipeline.Shape) bool {
	pipeline.EnsureHashes(&shape)
	return k.keyHashes[shape.KeyNamesHash] && k.propHashes[shape.PropertyHash]
}

func (k *KnownShape) Merge(other *KnownShape) {

	k.keyHashes.merge(other.keyHashes)
	k.propHashes.merge(other.propHashes)

	seenProps := map[string]bool{}
	allProps := []pipeline.PropertyDefinition{}

	for _, p := range append(other.Properties, k.Properties...) {
		if _, ok := seenProps[p.Name]; !ok {
			allProps = append(allProps, p)
			seenProps[p.Name] = true
		}
	}
	k.Properties = allProps

	seenKeys := map[string]bool{}
	allKeys := []string{}

	for _, p := range append(other.Keys, k.Keys...) {
		if _, ok := seenKeys[p]; !ok {
			allKeys = append(allKeys, p)
			seenKeys[p] = true
		}
	}
	k.Keys = allKeys

	k.cache = map[string]interface{}{}
}

// NewKnownShape creates a new KnownShape from a datapoint.
func NewKnownShape(datapoint pipeline.DataPoint) *KnownShape {

	shape := datapoint.Shape
	pipeline.EnsureHashes(&shape)

	ks := KnownShape{
		cache:      make(map[string]interface{}),
		keyHashes:  knownHashes{shape.KeyNamesHash: true},
		propHashes: knownHashes{shape.PropertyHash: true},
		ShapeDefinition: pipeline.ShapeDefinition{
			Name: canonicalName(datapoint),
			Keys: datapoint.Shape.KeyNames,
		},
	}

	for _, v := range datapoint.Shape.Properties {

		p := pipeline.PropertyDefinition{}
		p.Name, p.Type = utils.StringSplit2(v, ":")

		ks.Properties = append(ks.Properties, p)
	}

	sort.Sort(pipeline.SortPropertyDefinitionsByName(ks.Properties))

	return &ks

}

type ShapeCache struct {
	shapes map[string]*KnownShape
}

func NewShapeCache() ShapeCache {
	return ShapeCache{
		shapes: map[string]*KnownShape{},
	}
}

// Recognize returns the KnownShape for a datapoint if it's recognized.
// Otherwise it returns an empty KnownShape and recognized == false.
func (s *ShapeCache) Recognize(datapoint pipeline.DataPoint) (shape *KnownShape, recognized bool) {

	name := canonicalName(datapoint)
	shape, ok := s.shapes[name]

	if !ok || !shape.MatchesShape(datapoint.Shape) {
		return nil, false
	}

	return shape, true
}

func (s *ShapeCache) Analyze(datapoint pipeline.DataPoint) (shape *KnownShape, delta ShapeDelta) {

	shape = NewKnownShape(datapoint)

	oldShape, ok := s.shapes[shape.Name]

	knownShapes := map[string]pipeline.ShapeDefinition{}

	if ok {
		knownShapes[shape.Name] = oldShape.ShapeDefinition
	}

	delta = GenerateShapeDelta(knownShapes, shape.ShapeDefinition)

	return
}

// Remember merges the provided newShape into the cache, and returns
// the remembered shape (which may not be the shape that was merged in).
func (s *ShapeCache) Remember(newShape *KnownShape) (shape *KnownShape) {

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

func (si ShapeDelta) HasChanges() bool {
	return si.IsNew || si.HasKeyChanges || si.HasNewProperties
}

// GenerateShapeDelta will determine the diffferences between an existing shape and the shape of a new
// data point.  If the new shape is a subset of the current shape it is not considered a change.  This
// is due to the fact that it does not represent a change that needs to be made in the storage system.

func GenerateShapeDelta(knownShapes map[string]pipeline.ShapeDefinition, shapeDef pipeline.ShapeDefinition) ShapeDelta {

	// create the info
	info := ShapeDelta{
		Name: shapeDef.Name,

		//ShapeDef: shapeDef,
	}

	// Get the shape if we already know about it
	prevShape, ok := knownShapes[shapeDef.Name]

	// If this shape does not exist previously then
	// we need to treat it as brand new
	if !ok {
		info.IsNew = true
		info.ExistingKeys = []string{}
	} else {
		info.ExistingKeys = prevShape.Keys
	}

	// There aren't likely to be many keys, so we brute force this check
	for _, key := range shapeDef.Keys {
		if !contains(prevShape.Keys, key) {
			info.NewKeys = append(info.NewKeys, key)
			info.HasKeyChanges = true
		}
	}

	// Properties already known, no need to change.
	if isSubsetOf(shapeDef.Properties, prevShape.Properties) {
		if !info.HasKeyChanges {
			// No key changes and no property changes means we can just re-use the existing shape.
			//info.ShapeDef = prevShape
		}
		return info
	}

	info.NewProperties = PropertiesAndTypes{}
	info.HasNewProperties = true

	for _, prop := range shapeDef.Properties {
		if !containsProp(prop, prevShape.Properties) {
			info.NewProperties[prop.Name] = prop.Type
		}
	}

	return info
}

// contains is a helper function to determine if a string slice
// contains a string value
func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}

// areSame is a helper function that determines if two slices are
// the same.  Two slices are considered the same if they are the same
// length and contain equal values at the same indexes.
func areSame(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// isSubsetOf is a helper function that determines if one slice
// is a subset of another
func isSubsetOf(list []pipeline.PropertyDefinition, all []pipeline.PropertyDefinition) bool {
	for _, l := range list {
		if !containsProp(l, all) {
			return false
		}
	}
	return true
}

func containsProp(v pipeline.PropertyDefinition, a []pipeline.PropertyDefinition) bool {
	for _, i := range a {
		if i.Name == v.Name {
			return true
		}
	}
	return false
}

func canonicalName(dp pipeline.DataPoint) string {
	if dp.Source == "" {
		return dp.Entity
	}
	if dp.Entity == "" {
		return dp.Source
	}

	return dp.Source + "." + dp.Entity
}
