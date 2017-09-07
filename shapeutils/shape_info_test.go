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

func BenchmarkGenerateShapeInfo(b *testing.B) {
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

		_ = GenerateShapeInfo(known, shape)
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

func TestGenerateShapeInfo(t *testing.T) {

	Convey("Given a data point with a shape that does not exists in Subscriber.Shapes", t, func() {

		shapeInfo := GenerateShapeInfo(knownShapes, testShape)

		Convey("Should return a shape info", func() {
			Convey("with IsNew = true", func() {
				So(shapeInfo.IsNew, ShouldBeTrue)
			})
			Convey("with NewName", func() {
				So(shapeInfo.NewName, ShouldEqual, testShape.Name)
			})
			Convey("where HasChanges() returns true", func() {
				So(shapeInfo.HasChanges(), ShouldBeTrue)
			})
			Convey("with HasKeyChanges = true", func() {
				So(shapeInfo.HasKeyChanges, ShouldBeTrue)
			})
			Convey("with HasNewProperties = true", func() {
				So(shapeInfo.HasNewProperties, ShouldBeTrue)
			})
			Convey("with PreviousShape set to empty shape", func() {
				So(shapeInfo.PreviousShapeDef.Keys, ShouldBeEmpty)
				So(shapeInfo.PreviousShapeDef.Properties, ShouldBeEmpty)
			})
			Convey("with ShapeDef set to the provided shape", func() {
				So(shapeInfo.ShapeDef, ShouldResemble, testShape)
			})
			Convey("with NewKeys = ['id']", func() {
				So(shapeInfo.NewKeys, ShouldResemble, []string{"id"})
			})
			Convey("with NewProperties = ['age':'number','id':'number','name':'string']", func() {
				So(shapeInfo.NewProperties, ShouldResemble, PropertiesAndTypes{
					"age":  "number",
					"id":   "number",
					"name": "string",
				})
			})
		})

	})

	Convey("Given a data point with a shape that is exactly the same as an existing shape in Subscriber.Shapes", t, func() {

		knownShapes[testShape.Name] = testShape
		shapeInfo := GenerateShapeInfo(knownShapes, testShape)

		Reset(func() {
			knownShapes = map[string]pipeline.ShapeDefinition{}
		})

		Convey("Should return a shape info", func() {
			Convey("with IsNew = false", func() {
				So(shapeInfo.IsNew, ShouldBeFalse)
			})
			Convey("where HasChanges() returns false", func() {
				So(shapeInfo.HasChanges(), ShouldBeFalse)
			})
			Convey("with HasKeyChanges = false", func() {
				So(shapeInfo.HasKeyChanges, ShouldBeFalse)
			})
			Convey("with HasNewProperties = false", func() {
				So(shapeInfo.HasNewProperties, ShouldBeFalse)
			})
			Convey("with PreviousShape set to existing shape", func() {
				So(shapeInfo.PreviousShapeDef, ShouldResemble, testShape)
			})
			Convey("with Shape set to data points shape", func() {
				So(shapeInfo.ShapeDef, ShouldResemble, testShape)
			})
			Convey("with NewKeys set to empty array", func() {
				So(shapeInfo.NewKeys, ShouldBeEmpty)
			})
			Convey("with NewProperties set to empty array", func() {
				So(shapeInfo.NewProperties, ShouldBeEmpty)
			})
		})

	})

	Convey("Given a data point with a shape that has new properties", t, func() {
		knownShapes[testShape.Name] = testShapeNoAge
		shapeInfo := GenerateShapeInfo(knownShapes, testShape)

		Reset(func() {
			knownShapes = map[string]pipeline.ShapeDefinition{}
		})

		Convey("Should return a shape info", func() {
			Convey("with IsNew = false", func() {
				So(shapeInfo.IsNew, ShouldBeFalse)
			})
			Convey("where HasChanges() returns true", func() {
				So(shapeInfo.HasChanges(), ShouldBeTrue)
			})
			Convey("with HasKeyChanges = false", func() {
				So(shapeInfo.HasKeyChanges, ShouldBeFalse)
			})
			Convey("with HasNewProperties = true", func() {
				So(shapeInfo.HasNewProperties, ShouldBeTrue)
			})
			Convey("with PreviousShape set to exising shape", func() {
				So(shapeInfo.PreviousShapeDef, ShouldResemble, testShapeNoAge)
			})
			Convey("with Shape set to data points shape", func() {
				So(shapeInfo.ShapeDef, ShouldResemble, testShape)
			})
			Convey("with NewKeys set to empty array", func() {
				So(shapeInfo.NewKeys, ShouldBeEmpty)
			})
			Convey("with NewProperties = ['age':'number']", func() {
				So(shapeInfo.NewProperties, ShouldResemble, PropertiesAndTypes{
					"age": "number",
				})
			})
		})
	})

	Convey("Given a data point with a shape that has fewer properties than existing shape", t, func() {
		knownShapes[testShapeNoAge.Name] = testShape
		shapeInfo := GenerateShapeInfo(knownShapes, testShapeNoAge)

		Reset(func() {
			knownShapes = map[string]pipeline.ShapeDefinition{}
		})

		Convey("Should return a shape info", func() {
			Convey("with IsNew = false", func() {
				So(shapeInfo.IsNew, ShouldBeFalse)
			})
			Convey("where HasChanges() returns false", func() {
				So(shapeInfo.HasChanges(), ShouldBeFalse)
			})
			Convey("with HasKeyChanges = false", func() {
				So(shapeInfo.HasKeyChanges, ShouldBeFalse)
			})
			Convey("with HasNewProperties = false", func() {
				So(shapeInfo.HasNewProperties, ShouldBeFalse)
			})
			Convey("with PreviousShape set to existing shape", func() {
				So(shapeInfo.PreviousShapeDef, ShouldResemble, testShape)
			})
			Convey("with Shape set to existing shape", func() {
				So(shapeInfo.ShapeDef, ShouldResemble, testShape)
			})
			Convey("with NewKeys set to empty array", func() {
				So(shapeInfo.NewKeys, ShouldBeEmpty)
			})
			Convey("with NewProperties set to empty array", func() {
				So(shapeInfo.NewProperties, ShouldBeEmpty)
			})
		})
	})

	Convey("Given a data point with different keys", t, func() {
		knownShapes[testShape.Name] = testShape
		testShapeNewKey := testShape
		testShapeNewKey.Keys = []string{"name"}
		shapeInfo := GenerateShapeInfo(knownShapes, testShapeNewKey)

		Reset(func() {
			knownShapes = map[string]pipeline.ShapeDefinition{}
		})

		Convey("Should return a shape info", func() {
			Convey("with IsNew = false", func() {
				So(shapeInfo.IsNew, ShouldBeFalse)
			})
			Convey("where HasChanges() returns true", func() {
				So(shapeInfo.HasChanges(), ShouldBeTrue)
			})
			Convey("with HasKeyChanges = true", func() {
				So(shapeInfo.HasKeyChanges, ShouldBeTrue)
			})
			Convey("with HasNewProperties = false", func() {
				So(shapeInfo.HasNewProperties, ShouldBeFalse)
			})
			Convey("with PreviousShape set to existing shape", func() {
				So(shapeInfo.PreviousShapeDef, ShouldResemble, testShape)
			})
			Convey("with Shape set to data points shape", func() {
				So(shapeInfo.ShapeDef, ShouldResemble, testShapeNewKey)
			})
			Convey("with NewKeys = 'name'", func() {
				So(shapeInfo.NewKeys, ShouldResemble, []string{"name"})
			})
			Convey("with NewProperties set to empty array", func() {
				So(shapeInfo.NewProperties, ShouldBeEmpty)
			})
		})
	})

}
