package shapeutils

import "github.com/naveego/api/types/pipeline"

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
	PreviousShape    KnownShape
	NewShape         KnownShape
	Name             string
	NewKeys          []string
	NewProperties    PropertiesAndTypes
}

func (si ShapeDelta) HasChanges() bool {
	return si.IsNew || si.HasKeyChanges || si.HasNewProperties
}

// GenerateShapeDelta will determine the diffferences between an existing shape and the shape of a new
// data point.  If the new shape is a subset of the current shape it is not considered a change.  This
// is due to the fact that it does not represent a change that needs to be made in the storage system.

func GenerateShapeDelta(prevShape *KnownShape, newShape KnownShape) ShapeDelta {

	// create the info
	info := ShapeDelta{
		Name:     newShape.Name,
		NewShape: newShape,
	}

	// If this shape does not exist previously then
	// we need to treat it as brand new
	if prevShape == nil {
		info.IsNew = true
	} else {
		info.PreviousShape = *prevShape
	}

	// There aren't likely to be many keys, so we brute force this check
	for _, key := range newShape.Keys {
		if prevShape == nil || !contains(prevShape.Keys, key) {
			info.NewKeys = append(info.NewKeys, key)
			info.HasKeyChanges = true
		}
	}

	if prevShape == nil || !isSubsetOf(newShape.Properties, prevShape.Properties) {
		info.NewProperties = PropertiesAndTypes{}
		info.HasNewProperties = true

		for _, prop := range newShape.Properties {
			if prevShape == nil || !containsProp(prop, prevShape.Properties) {
				info.NewProperties[prop.Name] = prop.Type
			}
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
	return dp.Entity
}
