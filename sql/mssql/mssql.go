package mssql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/pipeline-api/subscriber"
)

type Subscriber struct {
	db            *sql.DB                   // The connection to the database
	currentShapes map[string]pipeline.Shape // The current shapes
}

func (s *Subscriber) Init(ctx subscriber.Context) {

	ctx.Logger.Info("Initializing Subscriber")

	connectionString, ok := getStringSetting(ctx.Subscriber.Settings, "connectionString")
	if !ok {
		ctx.Logger.Fatal("The connectionString setting was not provided or was not a valid string")
	}

	db, err := sql.Open("mssql", connectionString)
	if err != nil {
		ctx.Logger.Fatalf("Could not connect to SQL Database: %v", err)
	}

	shapes, err := getShapesFromDb(ctx, db)
	if err != nil {
		ctx.Logger.Fatalf("Could not initialize the SQL connection: %v", err)
	}

	err = ensureSchema(ctx, db)
	if err != nil {
		ctx.Logger.Fatalf("Could not ensure schema: %v", err)
	}

	s.db = db
	s.currentShapes = shapes

}

func (s *Subscriber) Receive(ctx subscriber.Context, dataPoint pipeline.DataPoint) {

	_, exists := s.currentShapes["WellAttribute"]
	if !exists {
		err := createShape(ctx, s.db, "wellcast", "WellAttribute", dataPoint.Shape)
		if err != nil {
			ctx.Logger.Error("Could not create shape storage", err)
		}
		s.currentShapes["WellAttribute"] = dataPoint.Shape
	}

	err := upsertData(ctx, s.db, "wellcast", "WellAttribute", dataPoint)
	if err != nil {
		ctx.Logger.Errorf("Could not save data to database: %v", err)
	}

}

func ensureSchema(ctx subscriber.Context, db *sql.DB) error {

	exists, err := schemaExists(ctx, db)
	if err != nil {
		return err
	}

	if !exists {
		ctx.Logger.Infof("Creating Schema wellcast")
		_, err = db.Exec("create schema wellcast")
	}

	return err
}

func schemaExists(ctx subscriber.Context, db *sql.DB) (bool, error) {
	stmt, err := db.Prepare("select count(*) from sys.schemas where name='wellcast'")
	if err != nil {
		return false, err
	}
	defer stmt.Close()
	row := stmt.QueryRow()

	var count int64
	err = row.Scan(&count)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func getShapesFromDb(ctx subscriber.Context, db *sql.DB) (map[string]pipeline.Shape, error) {

	ctx.Logger.Infof("Getting existing shapes from database")

	rows, err := db.Query(`select c.name as col_name, ty.name as type_name
	from sys.tables t
	inner join sys.columns c on (t.object_id = c.object_id)
	inner join sys.schemas s on (t.schema_id = s.schema_id)
	inner join sys.types ty on (c.user_type_id = ty.user_type_id)
	where
	s.name = ?1
	and
	t.name = ?2
	order by c.name
	`, "wellcast", "wellheader")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	shapes := make(map[string]pipeline.Shape)
	properties := []string{}

	for rows.Next() {

		var colName string
		var typeName string
		err = rows.Scan(&colName, &typeName)
		if err != nil {
			return nil, err
		}

		shapeStr := fmt.Sprintf("%s:%s", colName, typeName)
		properties = append(properties, shapeStr)
	}

	if len(properties) > 0 {
		ctx.Logger.Debugf("Found Shape: Name=wellheader, Properties=%s", strings.Join(properties, ","))

		shape, err := pipeline.NewShapeFromProperties(properties)
		if err != nil {
			return shapes, err
		}
		shapes["wellheader"] = shape
	}
	return shapes, nil
}

func createShape(ctx subscriber.Context, db *sql.DB, schemaName, entity string, shape pipeline.Shape) error {
	createStmt := fmt.Sprintf("create table [%s].[%s] ( \n", schemaName, entity)

	for _, propAndType := range shape.Properties {

		sepIdx := strings.Index(propAndType, ":")
		prop := normalizePropertyName(propAndType[:sepIdx])
		propType := propAndType[(sepIdx + 1):]

		createStmt = createStmt + "[" + prop + "] "

		switch propType {
		case "number":
			createStmt = createStmt + " decimal(18,4) null,\n "
		case "date":
			createStmt = createStmt + " smalldatetime null, \n "
		case "bool":
			createStmt = createStmt + " bit null,\n "
		default:
			createStmt = createStmt + " nvarchar(512) null,\n "
		}
	}

	createStmt = createStmt[:len(createStmt)-2] + " )"

	_, err := db.Exec(createStmt)
	return err

}

func upsertData(ctx subscriber.Context, db *sql.DB, schemaName, entity string, dataPoint pipeline.DataPoint) error {
	var columnStr, valueStr string
	upsertStmt := fmt.Sprintf("insert into [%s].[%s]\n", schemaName, entity)

	for _, propAndType := range dataPoint.Shape.Properties {
		sepIdx := strings.Index(propAndType, ":")
		prop := normalizePropertyName(propAndType[:sepIdx])
		propType := propAndType[(sepIdx + 1):]

		columnStr = columnStr + "[" + prop + "],"

		rawValue, ok := dataPoint.Data[prop]
		if !ok || rawValue == nil {
			valueStr = valueStr + " NULL,"
		} else {
			switch propType {
			case "number":
				valueStr = valueStr + fmt.Sprintf("%f,", rawValue)
			default:
				valueStr = valueStr + fmt.Sprintf("'%v',", rawValue)
			}

		}
	}

	columnStr = columnStr[:len(columnStr)-1]
	valueStr = valueStr[:len(valueStr)-1]

	upsertStmt = upsertStmt + fmt.Sprintf("( %s ) VALUES ( %s )", columnStr, valueStr)

	_, err := db.Exec(upsertStmt)
	if err != nil {
		ctx.Logger.Debugf("%s", upsertStmt)
	}
	return err
}

func normalizePropertyName(propName string) string {
	propName = strings.Replace(propName, "[", "", -1)
	propName = strings.Replace(propName, "]", "", -1)
	return propName
}

func getStringSetting(settings map[string]interface{}, name string) (string, bool) {

	rawValue, ok := settings[name]
	if !ok {
		return "", false
	}

	val, ok := rawValue.(string)
	if !ok {
		return "", false
	}

	return val, true

}
