package shapeutils

import (
	"github.com/naveego/api/types/pipeline"
)

// PropertiesAndTypes is a map that contains the property name
// and the type.  The key will be the name of the property and
// the value will contain the type
type PropertiesAndTypes map[string]string

// ShapeInfo will contain information about the current data points
// shape, with respect to the pipeline shape for the same entity.
// This information can be used by the subcriber to alter its
// storage if necessary.
type ShapeInfo struct {
	IsNew            bool
	HasKeyChanges    bool
	HasNewProperties bool
	PreviousShapeDef pipeline.ShapeDefinition
	ShapeDef         pipeline.ShapeDefinition
	NewName          string
	NewKeys          []string
	NewProperties    PropertiesAndTypes
}

func (si ShapeInfo) HasChanges() bool {
	return si.IsNew || si.HasKeyChanges || si.HasNewProperties
}

// GenerateShapeInfo will determine the diffferences between an existing shape and the shape of a new
// data point.  If the new shape is a subset of the current shape it is not considered a change.  This
// is due to the fact that it does not represent a change that needs to be made in the storage system.

func GenerateShapeInfo(knownShapes map[string]pipeline.ShapeDefinition, shapeDef pipeline.ShapeDefinition) ShapeInfo {

	// create the info
	info := ShapeInfo{
		ShapeDef: shapeDef,
	}

	// Get the shape if we already know about it
	prevShape, ok := knownShapes[shapeDef.Name]

	// If this shape does not exist previously then
	// we need to treat it as brand new
	if !ok {
		info.IsNew = true
		info.NewName = shapeDef.Name
	}

	// There aren't likely to be many keys, so we brute force this check
	for _, key := range shapeDef.Keys {
		if !contains(prevShape.Keys, key) {
			info.NewKeys = append(info.NewKeys, key)
			info.HasKeyChanges = true
		}
	}

	// Set the previous shape on the info
	info.PreviousShapeDef = prevShape

	// Properties already known, no need to change.
	if isSubsetOf(shapeDef.Properties, prevShape.Properties) {
		if !info.HasKeyChanges {
			// No key changes and no property changes means we can just re-use the existing shape.
			info.ShapeDef = prevShape
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

// GenerateShapeInfo will determine the diffferences between an existing shape and the shape of a new
// data point.  If the new shape is a subset of the current shape it is not considered a change.  This
// is due to the fact that it does not represent a change that needs to be made in the storage system.
// This alternative to GenerateShapeInfo was an attempt to improve performance, but it isn't working.
func generateShapeInfoX(knownShapes map[string]pipeline.ShapeDefinition, shapeDef pipeline.ShapeDefinition) ShapeInfo {

	// create the info
	info := ShapeInfo{
		ShapeDef: shapeDef,
	}

	// Get the shape if we already know about it
	prevShape, ok := knownShapes[shapeDef.Name]

	// If this shape does not exist previously then
	// we need to treat it as brand new
	if !ok {
		info.IsNew = true
		info.NewName = shapeDef.Name
	}

	// There aren't likely to be many keys, so we brute force this check
	for _, key := range shapeDef.Keys {
		if !contains(prevShape.Keys, key) {
			info.NewKeys = append(info.NewKeys, key)
			info.HasKeyChanges = true
		}
	}

	info.NewProperties = PropertiesAndTypes{}

	// First we assume all properties are new
	for _, p := range shapeDef.Properties {
		info.NewProperties[p.Name] = p.Type
	}

	// Then we delete all properties we already know about
	for _, p := range prevShape.Properties {
		delete(info.NewProperties, p.Name)
	}

	// Set the previous shape on the info
	info.PreviousShapeDef = prevShape

	info.HasNewProperties = (len(info.NewProperties) > 0)

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
