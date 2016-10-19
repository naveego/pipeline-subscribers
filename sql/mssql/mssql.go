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

	ctx.Logger.Debug("MSSQL Subscriber: Receiving Message")

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
				ctx.Logger.Warnf("Could not add property '%s' to entity '%s': %v", prop, dataPoint.Entity, err)
			}
		}
	}

	ctx.Logger.Debug("Upserting Data")
	if err := upsertData(ctx, s.db, schemaName, dataPoint.Entity, dataPoint, shapeInfo.Shape); err != nil {
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

		// If prop type is unknown we cannot create it yet
		if propType == "unknown" {
			continue
		}

		createStmt = createStmt + "[" + prop + "] "

		switch propType {
		case "number":
			createStmt += " decimal(18,4)"
		case "date":
			createStmt += " smalldatetime"
		case "bool":
			createStmt += " bit"
		default:
			createStmt += " nvarchar(512)"
		}

		if contains(shape.KeyNames, prop) {
			createStmt += " NOT NULL,\n"
		} else {
			createStmt += " NULL,\n"
		}
	}

	// add the primary key
	pKeys := "[" + strings.Join(shape.KeyNames, "],[") + "]"
	createStmt += fmt.Sprintf("CONSTRAINT PK_%s_%s PRIMARY KEY CLUSTERED (%s)\n", schemaName, entity, pKeys)

	// Close it off
	createStmt += ")"

	_, err := db.Exec(createStmt)
	return err

}

func addProperty(ctx subscriber.Context, db *sql.DB, schemaName, entity, property, propType string) error {

	var propSQLType string

	switch propType {
	case "number":
		propSQLType = "decimal(18,4)"
	case "date":
		propSQLType = "smalldatetime"
	case "bool":
		propSQLType = "bit"
	default:
		propSQLType = "nvarchar(512)"
	}

	altrStmt := fmt.Sprintf("alter table [%s].[%s] add [%s] %s NULL", schemaName, entity, property, propSQLType)

	_, err := db.Exec(altrStmt)

	return err

}

func upsertData(ctx subscriber.Context, db *sql.DB, schemaName, entity string, dataPoint pipeline.DataPoint, shape pipeline.Shape) error {
	var columnStr, valueStr, updateStr, keyClauseStr string
	insertStmt := fmt.Sprintf("IF @@rowcount = 0 \ninsert into [%s].[%s]\n", schemaName, entity)
	updateStmt := fmt.Sprintf("update [%s].[%s] SET\n", schemaName, entity)

	for _, propAndType := range shape.Properties {
		sepIdx := strings.Index(propAndType, ":")
		prop := normalizePropertyName(propAndType[:sepIdx])
		propType := propAndType[(sepIdx + 1):]

		// if the prop type is unknown we cannot insert the data
		if propType == "unknown" {
			continue
		}

		isKey := contains(dataPoint.Shape.KeyNames, prop)

		columnStr += "[" + prop + "],"
		updateStr += "[" + prop + "] = "

		if isKey {
			keyClauseStr += "[" + prop + "] = "
		}

		rawValue, ok := dataPoint.Data[prop]
		if !ok || rawValue == nil {
			valueStr += " NULL,"
			updateStr += " NULL,"
		} else {
			switch propType {
			case "number":
				valueStr += fmt.Sprintf("%f,", rawValue)
				updateStr += fmt.Sprintf("%f,", rawValue)

				if isKey {
					keyClauseStr += fmt.Sprintf("%f AND ", rawValue)
				}
			default:
				valueStr += fmt.Sprintf("'%v',", rawValue)
				updateStr += fmt.Sprintf("'%v',", rawValue)

				if isKey {
					keyClauseStr += fmt.Sprintf("'%v' AND ", rawValue)
				}
			}
		}
	}

	// Trim off the trailing commas and "AND"s
	columnStr = columnStr[:len(columnStr)-1]
	valueStr = valueStr[:len(valueStr)-1]
	updateStr = updateStr[:len(updateStr)-1]
	keyClauseStr = keyClauseStr[:len(keyClauseStr)-4]

	updateStmt += fmt.Sprintf("%s WHERE %s", updateStr, keyClauseStr)
	insertStmt += fmt.Sprintf("( %s ) VALUES ( %s )", columnStr, valueStr)

	upsertStmt := fmt.Sprintf("%s\n %s", updateStmt, insertStmt)
	ctx.Logger.Debug("Running Upsert: " + updateStmt)
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
