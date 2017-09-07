package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/naveego/navigator-go/subscribers/protocol"

	"github.com/naveego/api/types/pipeline"

	"github.com/naveego/pipeline-subscribers/shapeutils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSafeFormat(t *testing.T) {

	execute := func(expected, input string, args ...string) {
		So(fmt.Sprintf(input, escapeArgs(args...)...), ShouldEqual, expected)
	}

	Convey("Should strip invalid chars", t, func() {
		execute("CREATE TABLE `DROP Database`", "CREATE TABLE `%s`", "`DROP Database")
		execute("CREATE TABLE `x.y`", "CREATE TABLE `%s`", "x.y")
	})

}

func TestCreateShapeChangeSQL(t *testing.T) {

	Convey("Given a shape", t, func() {

		shape := shapeutils.ShapeInfo{
			IsNew:   true,
			NewName: "test",
			ShapeDef: pipeline.ShapeDefinition{
				Name: "test",
				Keys: []string{"id"},
			},
			NewKeys: []string{"id"},
			NewProperties: map[string]string{
				"id":   "integer",
				"str":  "string",
				"date": "date",
			},
		}

		Convey("When the shape is new", func() {
			shape.IsNew = true

			Convey("Then the SQL should be a CREATE statement", nil)

			actual, err := createShapeChangeSQL(shape)
			So(err, ShouldBeNil)
			So(actual, ShouldEqual, e(`CREATE TABLE IF NOT EXISTS "test" (
	"date" DATETIME NULL,
	"id" INT(10) NOT NULL,
	"str" VARCHAR(1000) NULL,
	PRIMARY KEY ("id")
)`))

		})

		Convey("When the shape is not new", func() {
			shape.IsNew = false

			Convey("The the SQL should be an ALTER statement", nil)

			actual, err := createShapeChangeSQL(shape)
			So(err, ShouldBeNil)
			So(actual, ShouldEqual, e(`ALTER TABLE "test"
	ADD COLUMN IF NOT EXISTS "date" DATETIME NULL
	,ADD COLUMN IF NOT EXISTS "id" INT(10) NOT NULL
	,ADD COLUMN IF NOT EXISTS "str" VARCHAR(1000) NULL
	,DROP PRIMARY KEY
	,ADD PRIMARY KEY ("id");`))

		})
	})

}

func TestCreateUpsertSQL(t *testing.T) {

	Convey("Given a request", t, func() {

		request := protocol.ReceiveShapeRequest{
			Shape: pipeline.ShapeDefinition{

				Name: "Test.Products",
				Keys: []string{"ID"},
				Properties: []pipeline.PropertyDefinition{
					{Name: "DateAvailable", Type: "date"},
					{Name: "ID", Type: "integer"},
					{Name: "Name", Type: "string"},
					{Name: "Price", Type: "float"},
				},
			},
			DataPoint: pipeline.DataPoint{
				Data: map[string]interface{}{
					"ID":            1,
					"Name":          "First",
					"Price":         42.2,
					"DateAvailable": "2017-10-11",
				},
			},
		}

		json.NewEncoder(os.Stdout).Encode(request)

		Convey("When we generate upsert SQL", func() {

			actual, params, err := createUpsertSQL(request)
			Convey("Then there should be no error", nil)
			So(err, ShouldBeNil)
			Convey("Then the SQL should be correct", nil)
			So(actual, ShouldEqual, e(`INSERT INTO "Test.Products" ("DateAvailable", "ID", "Name", "Price")
	VALUES (?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		"DateAvailable" = VALUES("DateAvailable")
		,"Name" = VALUES("Name")
		,"Price" = VALUES("Price");`))
			Convey("Then the parameters should be in the correct order", nil)
			So(params, ShouldResemble, []interface{}{"2017-10-11", 1, "First", 42.2})
		})

	})
}

func e(s string) string {
	return strings.Replace(s, `"`, "`", -1)
}
