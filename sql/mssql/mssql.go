package mssql

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/naveego/api/pipeline/subscriber"
	"github.com/naveego/api/types/pipeline"
)

type Subscriber struct {
	db             *sql.DB  // The connection to the database
	ensuredSchemas []string // An array of schema names that have already been ensured by this subscriber
}

func NewSubscriber() subscriber.Subscriber {
	return &Subscriber{}
}

func (s *Subscriber) Init(ctx subscriber.Context) error {

	ctx.Logger.Info("Initializing Subscriber")

	connectionString, ok := getStringSetting(ctx.Subscriber.Settings, "connection_string")
	if !ok {
		ctx.Logger.Fatal("The connection_string setting was not provided or was not a valid string")
	}

	db, err := sql.Open("mssql", connectionString)
	if err != nil {
		ctx.Logger.Fatalf("Could not connect to SQL Database: %v", err)
		return err
	}

	s.db = db

	return nil
}

func (s *Subscriber) Receive(ctx subscriber.Context, shapeInfo subscriber.ShapeInfo, dataPoint pipeline.DataPoint) {

	schemaName := getSchemaName(dataPoint)
	if !contains(s.ensuredSchemas, schemaName) {
		if err := ensureSchema(ctx, s.db, schemaName); err != nil {
			ctx.Logger.Errorf("Could not ensure schema '%s': %v", schemaName, err)
			return
		}
		s.ensuredSchemas = append(s.ensuredSchemas, schemaName)
	}

	if shapeInfo.IsNew {
		err := createShape(ctx, s.db, schemaName, dataPoint.Entity, shapeInfo.Shape)
		if err != nil {
			ctx.Logger.Errorf("Could not create shape for '%s': %v", dataPoint.Entity, err)
			return
		}
	} else if shapeInfo.HasNewProperties {

		for prop, pType := range shapeInfo.NewProperties {
			err := addProperty(ctx, s.db, schemaName, dataPoint.Entity, prop, pType)
			if err != nil {
				ctx.Logger.Errorf("Could not add property '%s' to entity '%s': %v", prop, dataPoint.Entity, err)
				return
			}
		}
	}

	if err := upsertData(ctx, s.db, schemaName, dataPoint.Entity, dataPoint); err != nil {
		ctx.Logger.Errorf("Could not upsert data point to entity '%s': %v", dataPoint.Entity, err)
	}
}

func getSchemaName(dataPoint pipeline.DataPoint) string {
	if dataPoint.Source == "" {
		return "dbo"
	}
	return strings.ToLower(dataPoint.Source)
}

func ensureSchema(ctx subscriber.Context, db *sql.DB, schemaName string) error {

	exists, err := schemaExists(ctx, db, schemaName)
	if err != nil {
		return err
	}

	if !exists {
		ctx.Logger.Infof("Creating Schema %s", schemaName)
		_, err = db.Exec(fmt.Sprintf("create schema [%s]", schemaName))
	}

	return err
}

func schemaExists(ctx subscriber.Context, db *sql.DB, schemaName string) (bool, error) {
	stmt, err := db.Prepare(fmt.Sprintf("select count(*) from sys.schemas where name='%s'", schemaName))
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

func addProperty(ctx subscriber.Context, db *sql.DB, schemaName, entity, property, propType string) error {

	propSQLType := "nvarchar(512)"

	switch propType {
	case "number":
		propSQLType = "decimal(18,4)"
	case "date":
		propSQLType = "smalldatetime"
	case "bool":
		propSQLType = "bit"
	}

	altrStmt := fmt.Sprintf("alter table [%s].[%s] add column [%s] %s NULL", schemaName, entity, property, propSQLType)

	_, err := db.Exec(altrStmt)
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

func contains(a []string, value string) bool {
	for _, v := range a {
		if v == value {
			return true
		}
	}
	return false
}
