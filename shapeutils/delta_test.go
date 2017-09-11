package shapeutils

import (
	"math/rand"
	"testing"

	"bitbucket.org/naveego/core/crypto"
	"github.com/naveego/api/types/pipeline"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testShape = KnownShape{
		ShapeDefinition: pipeline.ShapeDefinition{
			Name: "testShape",
			Keys: []string{"id"},
			Properties: []pipeline.PropertyDefinition{
				{Name: "age", Type: "number"},
				{Name: "id", Type: "number"},
				{Name: "name", Type: "string"},
			},
		},
	}

	testShapeNoAge = KnownShape{
		ShapeDefinition: pipeline.ShapeDefinition{
			Name: "testShapeNoAge",
			Keys: []string{"id"},
			Properties: []pipeline.PropertyDefinition{
				{Name: "id", Type: "number"},
				{Name: "name", Type: "string"},
			},
		},
	}
)

func BenchmarkGenerateShapeDelta(b *testing.B) {
	shapeCount := 100
	all := []KnownShape{}
	known := map[string]*KnownShape{}

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

			known[knownShape.Name] = &knownShape
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
		knownShape := known[shape.Name]

		_ = GenerateShapeDelta(knownShape, shape)
	}

}

func generateRandomShape() KnownShape {

	d := KnownShape{ShapeDefinition: pipeline.ShapeDefinition{
		Name: generateRandomString(),
	}}

	addPropertiesToShape(&d)

	return d
}

func addKeysToShape(d *KnownShape) {
	keyCount := rand.Intn(8) - 4
	for i := 0; i < keyCount; i++ {
		d.Keys = append(d.Keys, generateRandomString())
	}
}

func addPropertiesToShape(d *KnownShape) {
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

		ShapeDelta := GenerateShapeDelta(nil, testShape)

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

		ShapeDelta := GenerateShapeDelta(&testShape, testShape)

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

		ShapeDelta := GenerateShapeDelta(&testShapeNoAge, testShape)

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

		ShapeDelta := GenerateShapeDelta(&testShape, testShapeNoAge)

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
		testShapeNewKey := testShape
		testShapeNewKey.Keys = []string{"name"}
		ShapeDelta := GenerateShapeDelta(&testShape, testShapeNewKey)

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
