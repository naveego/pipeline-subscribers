package shapeutils

import (
	"math/rand"
	"testing"

	"bitbucket.org/naveego/core/crypto"

	"github.com/naveego/api/types/pipeline"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	knownShapes = map[string]pipeline.ShapeDefinition{}

	testShape = pipeline.ShapeDefinition{
		Name: "testShape",
		Keys: []string{"id"},
		Properties: []pipeline.PropertyDefinition{
			{Name: "age", Type: "number"},
			{Name: "id", Type: "number"},
			{Name: "name", Type: "string"},
		},
	}

	testShapeNoAge = pipeline.ShapeDefinition{
		Name: "testShapeNoAge",
		Keys: []string{"id"},
		Properties: []pipeline.PropertyDefinition{
			{Name: "id", Type: "number"},
			{Name: "name", Type: "string"},
		},
	}
)

func BenchmarkGenerateShapeDelta(b *testing.B) {
	shapeCount := 100
	all := []pipeline.ShapeDefinition{}
	known := map[string]pipeline.ShapeDefinition{}

	for i := 0; i < shapeCount; i++ {
		shape := generateRandomShape()

		if i%2 == 0 {

			knownShape := shape
			if i%5 == 0 {
				addPropertiesToShape(&knownShape)
			}
			if i%7 == 0 {
				addKeysToShape(&knownShape)
			}

			known[knownShape.Name] = knownShape
		}

		if i%7 == 0 {
			addPropertiesToShape(&shape)
		}

		if i%5 == 0 {
			addKeysToShape(&shape)
		}

		all = append(all, shape)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		index := i % shapeCount
		shape := all[index]

		_ = GenerateShapeDelta(known, shape)
	}

}

func generateRandomShape() pipeline.ShapeDefinition {

	d := pipeline.ShapeDefinition{
		Name: generateRandomString(),
	}

	addPropertiesToShape(&d)

	return d
}

func addKeysToShape(d *pipeline.ShapeDefinition) {
	keyCount := rand.Intn(8) - 4
	for i := 0; i < keyCount; i++ {
		d.Keys = append(d.Keys, generateRandomString())
	}
}

func addPropertiesToShape(d *pipeline.ShapeDefinition) {
	propCount := rand.Intn(30) - 10
	for i := 0; i < propCount; i++ {
		prop := pipeline.PropertyDefinition{
			Name: generateRandomString(),
			Type: generateRandomString(),
		}
		d.Properties = append(d.Properties, prop)
	}
}

func generateRandomString() string {
	s, _ := crypto.GenerateRandomString(20)
	return s
}

func TestGenerateShapeDelta(t *testing.T) {

	Convey("Given a data point with a shape that does not exists in Subscriber.Shapes", t, func() {

		ShapeDelta := GenerateShapeDelta(knownShapes, testShape)

		Convey("Should return a shape info", func() {
			Convey("with IsNew = true", func() {
				So(ShapeDelta.IsNew, ShouldBeTrue)
			})
			Convey("with Name", func() {
				So(ShapeDelta.Name, ShouldEqual, testShape.Name)
			})
			Convey("where HasChanges() returns true", func() {
				So(ShapeDelta.HasChanges(), ShouldBeTrue)
			})
			Convey("with HasKeyChanges = true", func() {
				So(ShapeDelta.HasKeyChanges, ShouldBeTrue)
			})
			Convey("with HasNewProperties = true", func() {
				So(ShapeDelta.HasNewProperties, ShouldBeTrue)
			})
			// Convey("with PreviousShape set to empty shape", func() {
			// 	So(ShapeDelta.PreviousShapeDef.Keys, ShouldBeEmpty)
			// 	So(ShapeDelta.PreviousShapeDef.Properties, ShouldBeEmpty)
			// })
			// Convey("with ShapeDef set to the provided shape", func() {
			// 	So(ShapeDelta.ShapeDef, ShouldResemble, testShape)
			// })
			Convey("with NewKeys = ['id']", func() {
				So(ShapeDelta.NewKeys, ShouldResemble, []string{"id"})
			})
			Convey("with NewProperties = ['age':'number','id':'number','name':'string']", func() {
				So(ShapeDelta.NewProperties, ShouldResemble, PropertiesAndTypes{
					"age":  "number",
					"id":   "number",
					"name": "string",
				})
			})
		})

	})

	Convey("Given a data point with a shape that is exactly the same as an existing shape in Subscriber.Shapes", t, func() {

		knownShapes[testShape.Name] = testShape
		ShapeDelta := GenerateShapeDelta(knownShapes, testShape)

		Reset(func() {
			knownShapes = map[string]pipeline.ShapeDefinition{}
		})

		Convey("Should return a shape info", func() {
			Convey("with IsNew = false", func() {
				So(ShapeDelta.IsNew, ShouldBeFalse)
			})
			Convey("with Name", func() {
				So(ShapeDelta.Name, ShouldEqual, testShape.Name)
			})
			Convey("where HasChanges() returns false", func() {
				So(ShapeDelta.HasChanges(), ShouldBeFalse)
			})
			Convey("with HasKeyChanges = false", func() {
				So(ShapeDelta.HasKeyChanges, ShouldBeFalse)
			})
			Convey("with HasNewProperties = false", func() {
				So(ShapeDelta.HasNewProperties, ShouldBeFalse)
			})
			// Convey("with PreviousShape set to existing shape", func() {
			// 	So(ShapeDelta.PreviousShapeDef, ShouldResemble, testShape)
			// })
			// Convey("with Shape set to data points shape", func() {
			// 	So(ShapeDelta.ShapeDef, ShouldResemble, testShape)
			// })
			Convey("with NewKeys set to empty array", func() {
				So(ShapeDelta.NewKeys, ShouldBeEmpty)
			})
			Convey("with NewProperties set to empty array", func() {
				So(ShapeDelta.NewProperties, ShouldBeEmpty)
			})
		})

	})

	Convey("Given a data point with a shape that has new properties", t, func() {
		knownShapes[testShape.Name] = testShapeNoAge
		ShapeDelta := GenerateShapeDelta(knownShapes, testShape)

		Reset(func() {
			knownShapes = map[string]pipeline.ShapeDefinition{}
		})

		Convey("Should return a shape info", func() {
			Convey("with IsNew = false", func() {
				So(ShapeDelta.IsNew, ShouldBeFalse)
			})
			Convey("where HasChanges() returns true", func() {
				So(ShapeDelta.HasChanges(), ShouldBeTrue)
			})
			Convey("with Name", func() {
				So(ShapeDelta.Name, ShouldEqual, testShape.Name)
			})
			Convey("with HasKeyChanges = false", func() {
				So(ShapeDelta.HasKeyChanges, ShouldBeFalse)
			})
			Convey("with HasNewProperties = true", func() {
				So(ShapeDelta.HasNewProperties, ShouldBeTrue)
			})
			// Convey("with PreviousShape set to exising shape", func() {
			// 	So(ShapeDelta.PreviousShapeDef, ShouldResemble, testShapeNoAge)
			// })
			// Convey("with Shape set to data points shape", func() {
			// 	So(ShapeDelta.ShapeDef, ShouldResemble, testShape)
			// })
			Convey("with NewKeys set to empty array", func() {
				So(ShapeDelta.NewKeys, ShouldBeEmpty)
			})
			Convey("with NewProperties = ['age':'number']", func() {
				So(ShapeDelta.NewProperties, ShouldResemble, PropertiesAndTypes{
					"age": "number",
				})
			})
		})
	})

	Convey("Given a data point with a shape that has fewer properties than existing shape", t, func() {
		knownShapes[testShapeNoAge.Name] = testShape
		ShapeDelta := GenerateShapeDelta(knownShapes, testShapeNoAge)

		Reset(func() {
			knownShapes = map[string]pipeline.ShapeDefinition{}
		})

		Convey("Should return a shape info", func() {
			Convey("with IsNew = false", func() {
				So(ShapeDelta.IsNew, ShouldBeFalse)
			})
			Convey("where HasChanges() returns false", func() {
				So(ShapeDelta.HasChanges(), ShouldBeFalse)
			})
			Convey("with HasKeyChanges = false", func() {
				So(ShapeDelta.HasKeyChanges, ShouldBeFalse)
			})
			Convey("with HasNewProperties = false", func() {
				So(ShapeDelta.HasNewProperties, ShouldBeFalse)
			})
			// Convey("with PreviousShape set to existing shape", func() {
			// 	So(ShapeDelta.PreviousShapeDef, ShouldResemble, testShape)
			// })
			// Convey("with Shape set to existing shape", func() {
			// 	So(ShapeDelta.ShapeDef, ShouldResemble, testShape)
			// })
			Convey("with NewKeys set to empty array", func() {
				So(ShapeDelta.NewKeys, ShouldBeEmpty)
			})
			Convey("with NewProperties set to empty array", func() {
				So(ShapeDelta.NewProperties, ShouldBeEmpty)
			})
		})
	})

	Convey("Given a data point with different keys", t, func() {
		knownShapes[testShape.Name] = testShape
		testShapeNewKey := testShape
		testShapeNewKey.Keys = []string{"name"}
		ShapeDelta := GenerateShapeDelta(knownShapes, testShapeNewKey)

		Reset(func() {
			knownShapes = map[string]pipeline.ShapeDefinition{}
		})

		Convey("Should return a shape info", func() {
			Convey("with IsNew = false", func() {
				So(ShapeDelta.IsNew, ShouldBeFalse)
			})
			Convey("where HasChanges() returns true", func() {
				So(ShapeDelta.HasChanges(), ShouldBeTrue)
			})
			Convey("with HasKeyChanges = true", func() {
				So(ShapeDelta.HasKeyChanges, ShouldBeTrue)
			})
			Convey("with HasNewProperties = false", func() {
				So(ShapeDelta.HasNewProperties, ShouldBeFalse)
			})
			// Convey("with PreviousShape set to existing shape", func() {
			// 	So(ShapeDelta.PreviousShapeDef, ShouldResemble, testShape)
			// })
			// Convey("with Shape set to data points shape", func() {
			// 	So(ShapeDelta.ShapeDef, ShouldResemble, testShapeNewKey)
			// })
			Convey("with NewKeys = 'name'", func() {
				So(ShapeDelta.NewKeys, ShouldResemble, []string{"name"})
			})
			Convey("with NewProperties set to empty array", func() {
				So(ShapeDelta.NewProperties, ShouldBeEmpty)
			})
		})
	})

}

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

func Test_ShapeCache(t *testing.T) {

	Convey("Given a ShapeCache", t, func() {
		sut := NewShapeCache()

		Convey("When a datapoint is not recognized", func() {
			dp := pipeline.DataPoint{
				Source: "Test",
				Entity: "Products",
				Shape: pipeline.Shape{
					KeyNames:     []string{"id"},
					KeyNamesHash: 123,
					PropertyHash: 456,
					Properties:   []string{"id:integer", "name:string"},
				},
			}

			Convey("Then the cache returns false", func() {
				_, ok := sut.Recognize(dp)
				So(ok, ShouldBeFalse)
			})

			Convey("Then the cache can produce a delta", func() {
				shape, delta := sut.Analyze(dp)
				So(shape, ShouldResemble, &KnownShape{
					cache:      map[string]interface{}{},
					keyHashes:  knownHashes{123: true},
					propHashes: knownHashes{456: true},
					ShapeDefinition: pipeline.ShapeDefinition{

						Name: "Test.Products",
						Keys: []string{"id"},
						Properties: []pipeline.PropertyDefinition{
							{Name: "id", Type: "integer"},
							{Name: "name", Type: "string"},
						},
					},
				})
				So(delta, ShouldResemble, ShapeDelta{
					IsNew:            true,
					HasKeyChanges:    true,
					HasNewProperties: true,
					Name:             "Test.Products",
					NewKeys:          []string{"id"},
					ExistingKeys:     []string{},
					NewProperties: PropertiesAndTypes{
						"id":   "integer",
						"name": "string",
					},
				})
			})

			Convey("Then the cache can accept an update.", func() {
				expected := &KnownShape{
					cache:      map[string]interface{}{},
					keyHashes:  knownHashes{123: true},
					propHashes: knownHashes{456: true},
					ShapeDefinition: pipeline.ShapeDefinition{

						Name: "Test.Products",
						Keys: []string{"id"},
						Properties: []pipeline.PropertyDefinition{
							{Name: "id", Type: "integer"},
							{Name: "name", Type: "string"},
						},
					},
				}

				actual := sut.Remember(expected)
				So(actual, ShouldResemble, expected)
			})

		})

		Convey("When a datapoint is  recognized", func() {
			dp := pipeline.DataPoint{
				Source: "Test",
				Entity: "Products",
				Shape: pipeline.Shape{
					KeyNames:     []string{"id"},
					KeyNamesHash: 123,
					PropertyHash: 456,
					Properties:   []string{"id:integer", "name:string"},
				},
			}

			expected := &KnownShape{
				cache:      map[string]interface{}{},
				keyHashes:  knownHashes{123: true},
				propHashes: knownHashes{456: true},
				ShapeDefinition: pipeline.ShapeDefinition{

					Name: "Test.Products",
					Keys: []string{"id"},
					Properties: []pipeline.PropertyDefinition{
						{Name: "id", Type: "integer"},
						{Name: "name", Type: "string"},
					},
				},
			}

			sut.Remember(expected)

			Convey("Then the cache returns true", func() {
				actual, ok := sut.Recognize(dp)

				So(ok, ShouldBeTrue)
				So(actual, ShouldResemble, expected)
			})

		})
	})

}
