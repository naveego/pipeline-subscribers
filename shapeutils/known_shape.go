package shapeutils

import (
	"sort"

	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/api/utils"
)

type knownHashes map[uint32]bool

func (k knownHashes) merge(other knownHashes) {
	for h, v := range other {
		k[h] = v
	}
}

type KnownShape struct {
	pipeline.ShapeDefinition

	cache map[string]interface{}

	keyHashes          knownHashes       // contains all the key hashes this shape contains.
	propHashes         knownHashes       // contains all the property hashes this shape contains.
	allKeys            map[string]bool   // contains all the key strings this shape contains, used to recognize subsets of keys quickly.
	allPropertyStrings map[string]bool   // contains all the name:type property strings this shape has observed, used to recognize subsets of properties quickly.
	idToNameMap        map[string]string // maps the property IDs to friendly names
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

	if k.keyHashes[shape.KeyNamesHash] && k.propHashes[shape.PropertyHash] {
		// hashes match, shapes are OK
		return true
	}

	// If the hashes don't match, the shape may be a subset of the known shape.
	// In that case, we'll update the known shape so that it'll recognise
	// this shape quickly in the future.

	for _, key := range shape.KeyNames {
		if !k.allKeys[key] {
			return false
		}
	}

	for _, prop := range shape.Properties {
		if !k.allPropertyStrings[prop] {
			return false
		}
	}

	// keys and properties are a subset of known shape,
	// so remember the hashes so that we can be fast next time.
	k.keyHashes[shape.KeyNamesHash] = true
	k.propHashes[shape.PropertyHash] = true

	return true
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

	for key := range other.allKeys {
		k.allKeys[key] = true
	}

	for propString := range other.allPropertyStrings {
		k.allPropertyStrings[propString] = true
	}

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
		cache:              make(map[string]interface{}),
		allKeys:            make(map[string]bool),
		allPropertyStrings: make(map[string]bool),
		idToNameMap:        make(map[string]string),
		keyHashes:          knownHashes{shape.KeyNamesHash: true},
		propHashes:         knownHashes{shape.PropertyHash: true},
		ShapeDefinition: pipeline.ShapeDefinition{
			Name: canonicalName(datapoint),
			Keys: datapoint.Shape.KeyNames,
		},
	}

	for _, v := range datapoint.Shape.KeyNames {
		ks.allKeys[v] = true
	}

	for _, v := range datapoint.Shape.Properties {

		p := pipeline.PropertyDefinition{}
		p.Name, p.Type = utils.StringSplit2(v, ":")

		ks.allPropertyStrings[v] = true
		ks.Properties = append(ks.Properties, p)
	}

	for k, v := range datapoint.Data {
		switch s := v.(type) {
		case string:
			ks.idToNameMap[k] = s
		}
	}

	sort.Sort(pipeline.SortPropertyDefinitionsByName(ks.Properties))

	return &ks

}
