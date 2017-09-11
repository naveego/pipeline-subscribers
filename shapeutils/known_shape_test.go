package shapeutils

import (
	"testing"

	"github.com/naveego/api/types/pipeline"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_KnownShape_MatchesShape(t *testing.T) {
	Convey("Given a known shape", t, func() {
		sut := KnownShape{
			keyHashes:  knownHashes{123: true},
			propHashes: knownHashes{456: true},
		}
		Convey("When compared to a matching shape", func() {
			actual := sut.MatchesShape(pipeline.Shape{
				KeyNamesHash: 123,
				PropertyHash: 456,
			})
			Convey("Should return true", func() {
				So(actual, ShouldBeTrue)
			})
		})

		Convey("When compared to a different shape", func() {
			actual := sut.MatchesShape(pipeline.Shape{
				KeyNamesHash: 123,
				PropertyHash: 456,
			})
			Convey("Should return false", func() {
				So(actual, ShouldBeTrue)
			})
		})
	})
}

func Test_NewKnownShape(t *testing.T) {

	Convey("Given a datapoint", t, func() {

		dp := pipeline.DataPoint{
			Source: "test-source",
			Entity: "test-entity",
			Shape: pipeline.Shape{
				KeyNames:     []string{"id"},
				KeyNamesHash: 123,
				PropertyHash: 456,
				Properties:   []string{"id:number", "name:string"},
			},
		}

		Convey("When transformed to a KnownShape", func() {
			actual := NewKnownShape(dp)

			Convey("Should have stored hashes", func() {
				So(actual.keyHashes, ShouldContainKey, dp.Shape.KeyNamesHash)
				So(actual.propHashes, ShouldContainKey, dp.Shape.PropertyHash)
			})

			Convey("Should have created canonical name", func() {
				So(actual.Name, ShouldEqual, dp.Source+"."+dp.Entity)
			})

			Convey("Should be able to cache items", func() {
				actual.Set("x", "y")
				v, _ := actual.Get("x")
				So(v, ShouldEqual, "y")
			})

			Convey("Should get false on a cache miss", func() {
				_, ok := actual.Get("x")
				So(ok, ShouldBeFalse)
			})

			Convey("Should have parsed properties", func() {
				So(actual.Properties, ShouldResemble, []pipeline.PropertyDefinition{
					{Name: "id", Type: "number"},
					{Name: "name", Type: "string"},
				})
			})
		})
	})
}

func Test_KnownShape_Merge(t *testing.T) {

	Convey("Given a known shape", t, func() {

		self := KnownShape{
			cache:      map[string]interface{}{},
			keyHashes:  knownHashes{123: true},
			propHashes: knownHashes{456: true},
			ShapeDefinition: pipeline.ShapeDefinition{

				Name: "Test.Products",
				Keys: []string{"ID"},
				Properties: []pipeline.PropertyDefinition{
					{Name: "DateAvailable", Type: "date"},
					{Name: "ID", Type: "integer"},
					{Name: "Name", Type: "string"},
					{Name: "Price", Type: "float"},
				},
			},
		}

		other := KnownShape{
			cache:      map[string]interface{}{},
			keyHashes:  knownHashes{321: true},
			propHashes: knownHashes{654: true},
			ShapeDefinition: pipeline.ShapeDefinition{

				Name: "Test.Products",
				Keys: []string{"ID", "DI"},
				Properties: []pipeline.PropertyDefinition{
					{Name: "ID", Type: "integer"},
					{Name: "DI", Type: "integer"},
					{Name: "Mane", Type: "string"},
				},
			},
		}

		sut := self

		Convey("When another known shape is merged in", func() {

			sut.Set("x", "y")

			sut.Merge(&other)

			Convey("Should have all hashes", func() {
				So(sut.keyHashes, ShouldContainKey, uint32(123))
				So(sut.keyHashes, ShouldContainKey, uint32(321))
				So(sut.propHashes, ShouldContainKey, uint32(456))
				So(sut.propHashes, ShouldContainKey, uint32(654))
			})

			Convey("Should have cleared cache", func() {
				_, ok := sut.Get("x")
				So(ok, ShouldBeFalse)
			})

			Convey("Should contain all keys", func() {
				for _, x := range self.Keys {
					So(sut.Keys, ShouldContain, x)
				}
				for _, x := range other.Keys {
					So(sut.Keys, ShouldContain, x)
				}
				So(sut.Keys, ShouldHaveLength, 2)
			})

			Convey("Should contain all properties", func() {
				for _, x := range self.Properties {
					So(sut.Properties, ShouldContain, x)
				}
				for _, x := range other.Properties {
					So(sut.Properties, ShouldContain, x)
				}
				So(sut.Properties, ShouldHaveLength, 6)
			})
		})
	})
}
