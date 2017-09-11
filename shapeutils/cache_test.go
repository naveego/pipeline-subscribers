package shapeutils

import (
	"testing"

	"github.com/naveego/api/types/pipeline"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_ShapeCache(t *testing.T) {

	Convey("Given a ShapeCache and a DataPoint", t, func() {
		sut := NewShapeCache()

		dp := pipeline.DataPoint{
			Source: "Test",
			Entity: "Products",
			Shape: pipeline.Shape{
				KeyNames:   []string{"id"},
				Properties: []string{"id:integer", "name:string"},
			},
		}
		recalculateHashes(&dp)

		dpShape := &KnownShape{
			cache:              map[string]interface{}{},
			keyHashes:          knownHashes{dp.Shape.KeyNamesHash: true},
			propHashes:         knownHashes{dp.Shape.PropertyHash: true},
			allKeys:            map[string]bool{"id": true},
			allPropertyStrings: map[string]bool{"id:integer": true, "name:string": true},
			ShapeDefinition: pipeline.ShapeDefinition{

				Name: "Test.Products",
				Keys: []string{"id"},
				Properties: []pipeline.PropertyDefinition{
					{Name: "id", Type: "integer"},
					{Name: "name", Type: "string"},
				},
			},
		}

		Convey("When the datapoint has new shape", func() {

			Convey("Then the cache returns false", nil)
			_, ok := sut.GetKnownShape(dp)
			So(ok, ShouldBeFalse)

			Convey("Then the cache can produce a delta", nil)
			delta := sut.Analyze(dp)
			So(delta, ShouldResemble, ShapeDelta{
				IsNew:            true,
				HasKeyChanges:    true,
				HasNewProperties: true,
				Name:             "Test.Products",
				NewKeys:          []string{"id"},
				NewShape:         *dpShape,
				NewProperties: PropertiesAndTypes{
					"id":   "integer",
					"name": "string",
				},
			})

			Convey("Then the cache can apply the delta.", nil)

			actual := sut.ApplyDelta(delta)
			So(actual, ShouldResemble, dpShape)

			Convey("Then the cache recognizes the data point", nil)
			_, ok = sut.GetKnownShape(dp)
			So(ok, ShouldBeTrue)

		})

		Convey("When the datapoint has shape that has more properties than a known shape", func() {

			dpkeyhash := dp.Shape.KeyNamesHash
			dpprophash := dp.Shape.PropertyHash

			sut.shapes[canonicalName(dp)] = dpShape

			dp.Shape.Properties = append(dp.Shape.Properties, "other:bool")
			recalculateHashes(&dp)
			Convey("Then the cache returns false", nil)
			_, ok := sut.GetKnownShape(dp)
			So(ok, ShouldBeFalse)

			expectedNewShape := &KnownShape{
				cache:              map[string]interface{}{},
				keyHashes:          knownHashes{dp.Shape.KeyNamesHash: true, dpkeyhash: true},
				propHashes:         knownHashes{dp.Shape.PropertyHash: true, dpprophash: true},
				allKeys:            map[string]bool{"id": true},
				allPropertyStrings: map[string]bool{"id:integer": true, "name:string": true, "other:bool": true},
				ShapeDefinition: pipeline.ShapeDefinition{

					Name: "Test.Products",
					Keys: []string{"id"},
					Properties: []pipeline.PropertyDefinition{
						{Name: "id", Type: "integer"},
						{Name: "name", Type: "string"},
						{Name: "other", Type: "bool"},
					},
				},
			}

			Convey("Then the cache can produce a delta", nil)
			delta := sut.Analyze(dp)
			So(delta.IsNew, ShouldBeFalse)
			So(delta.HasKeyChanges, ShouldBeFalse)
			So(delta.HasNewProperties, ShouldBeTrue)

			Convey("Then the cache can apply the delta.", nil)

			actual := sut.ApplyDelta(delta)
			So(actual, ShouldResemble, expectedNewShape)

			Convey("Then the cache recognizes the data point", nil)
			_, ok = sut.GetKnownShape(dp)
			So(ok, ShouldBeTrue)

		})

		Convey("When the datapoint has shape that has more keys than a known shape", func() {

			dpkeyhash := dp.Shape.KeyNamesHash
			dpprophash := dp.Shape.PropertyHash

			sut.shapes[canonicalName(dp)] = dpShape

			dp.Shape.KeyNames = append(dp.Shape.KeyNames, "otherkey")
			recalculateHashes(&dp)

			Convey("Then the cache returns false", nil)
			_, ok := sut.GetKnownShape(dp)
			So(ok, ShouldBeFalse)

			expectedNewShape := &KnownShape{
				cache:              map[string]interface{}{},
				keyHashes:          knownHashes{dp.Shape.KeyNamesHash: true, dpkeyhash: true},
				propHashes:         knownHashes{dp.Shape.PropertyHash: true, dpprophash: true},
				allKeys:            map[string]bool{"id": true, "otherkey": true},
				allPropertyStrings: map[string]bool{"id:integer": true, "name:string": true},
				ShapeDefinition: pipeline.ShapeDefinition{

					Name: "Test.Products",
					Keys: []string{"id", "otherkey"},
					Properties: []pipeline.PropertyDefinition{
						{Name: "id", Type: "integer"},
						{Name: "name", Type: "string"},
					},
				},
			}

			Convey("Then the cache can produce a delta", nil)
			delta := sut.Analyze(dp)
			So(delta.IsNew, ShouldBeFalse)
			Convey("And the delta has key changes", nil)
			So(delta.HasKeyChanges, ShouldBeTrue)
			Convey("And the delta does not have property changes", nil)
			So(delta.HasNewProperties, ShouldBeFalse)

			Convey("Then the cache can apply the delta.", nil)

			actual := sut.ApplyDelta(delta)
			So(actual, ShouldResemble, expectedNewShape)

			Convey("Then the cache recognizes the data point", nil)
			_, ok = sut.GetKnownShape(dp)
			So(ok, ShouldBeTrue)

		})

		Convey("When a datapoint has properties that are a subset of a shape the cache has seen before", func() {
			dp.Shape = pipeline.Shape{
				KeyNames:   []string{"id"},
				Properties: []string{"id:integer"},
			}
			recalculateHashes(&dp)

			sut.shapes[canonicalName(dp)] = dpShape

			Convey("Then the cache returns true", func() {
				_, ok := sut.GetKnownShape(dp)
				So(ok, ShouldBeTrue)
			})

		})

		Convey("When the datapoint matches a known shape", func() {

			sut.shapes[canonicalName(dp)] = dpShape

			Convey("Then the cache returns true", func() {
				actual, ok := sut.GetKnownShape(dp)

				So(ok, ShouldBeTrue)
				So(actual, ShouldResemble, dpShape)
			})

		})
	})

}

func recalculateHashes(dp *pipeline.DataPoint) {
	dp.Shape.KeyNamesHash = 0
	dp.Shape.PropertyHash = 0
	pipeline.EnsureHashes(&dp.Shape)
}
