package oracle

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/naveego/api/pipeline/subscriber"
	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/api/utils"
)

type Subscriber struct {
	db             *sql.DB  // The connection to the database
	ensuredSchemas []string // An array of schema names that have already been ensured by this subscriber
}

func NewSubscriber() subscriber.Subscriber {
	return &Subscriber{}
}

func (p *Subscriber) TestConnection(ctx subscriber.Context, connSettings map[string]interface{}) (bool, string, error) {
	connString, err := buildConnectionString(connSettings, 10)
	if err != nil {
		return false, "could not connect to server", err
	}
	conn, err := sql.Open("ora", connString)
	if err != nil {
		return false, "could not connect to server", err
	}
	defer conn.Close()

	stmt, err := conn.Prepare("select 1")
	if err != nil {
		return false, "could not connect to server", err
	}
	defer stmt.Close()

	return true, "successfully connected to server", nil
}

func (p *Subscriber) Shapes(ctx subscriber.Context) (pipeline.ShapeDefinitions, error) {
	mr := utils.NewMapReader(ctx.Subscriber.Settings)
	cmdType, _ := mr.ReadString("command_type")
	if cmdType == "stored procedure" {
		return getSPShapes(ctx.Subscriber.Settings)
	}

	return getTableShapes(ctx.Subscriber.Settings)
}

func (s *Subscriber) Receive(ctx subscriber.Context, shape pipeline.ShapeDefinition, dataPoint pipeline.DataPoint) error {
	mr := utils.NewMapReader(ctx.Subscriber.Settings)
	cmdType, _ := mr.ReadString("command_type")
	if cmdType == "stored procedure" {
		return receiveShapeToSP(ctx, shape, dataPoint)
	}

	return nil
}

func receiveShapeToSP(ctx subscriber.Context, shape pipeline.ShapeDefinition, dataPoint pipeline.DataPoint) error {
	connString, err := buildConnectionString(ctx.Subscriber.Settings, 30)
	if err != nil {
		return err
	}
	conn, err := sql.Open("ora", connString)
	if err != nil {
		return err
	}
	defer conn.Close()

	schemaName := "dbo"
	spName := shape.Name

	if strings.Contains(shape.Name, "__") {
		idx := strings.Index(shape.Name, "__")
		schemaName = spName[:idx]
		spName = spName[idx+2:]
	}

	valCount := len(ctx.Pipeline.Mappings)
	vals := make([]interface{}, valCount)
	for i := 0; i < valCount; i++ {
		vals[i] = new(interface{})
	}

	params := []string{}
	index := 1
	for _, m := range ctx.Pipeline.Mappings {
		p := fmt.Sprintf(" %s = ?%d", m.To, index)
		params = append(params, p)

		if v, ok := dataPoint.Data[m.From]; ok {
			vals[index-1] = v
		}

		index++
	}

	paramsStr := strings.Join(params, ",")
	cmd := fmt.Sprintf("EXEC [%s].[%s] %s", schemaName, spName, paramsStr)
	logrus.Infof("Command: %s", cmd)
	_, e := conn.Exec(cmd, vals...)
	if e != nil {
		return e
	}

	return nil
}

func getSPShapes(settings map[string]interface{}) (pipeline.ShapeDefinitions, error) {
	q := `SELECT p.OBJECT_NAME, a.ARGUMENT_NAME, a.DATA_TYPE
FROM
SYS.USER_PROCEDURES p
INNER JOIN SYS.USER_ARGUMENTS a ON (p.OBJECT_NAME = a.OBJECT_NAME)
ORDER BY p.OBJECT_NAME, a.POSITION`

	return getShapes(settings, q)
}

func getTableShapes(settings map[string]interface{}) (pipeline.ShapeDefinitions, error) {
	q := `select s.Name, o.Name, c.Name, ty.name from
		sys.objects o
		INNER JOIN sys.schemas s ON (o.schema_id = s.schema_id)
		INNER JOIN sys.columns c ON (o.object_id = c.object_id)
		INNER JOIN sys.types ty ON (c.user_type_id = ty.user_type_id)
		where type IN ('U', 'V')
		ORDER BY s.Name, o.Name, c.column_id`

	return getShapes(settings, q)
}

func getShapes(settings map[string]interface{}, query string) (pipeline.ShapeDefinitions, error) {
	defs := pipeline.ShapeDefinitions{}

	connString, err := buildConnectionString(settings, 30)
	if err != nil {
		return defs, err
	}
	conn, err := sql.Open("ora", connString)
	if err != nil {
		return defs, err
	}
	defer conn.Close()

	rows, err := conn.Query(query)

	if err != nil {
		return defs, err
	}
	defer rows.Close()

	var tableName string
	var columnName string
	var columnType string

	s := map[string]*pipeline.ShapeDefinition{}

	for rows.Next() {
		err = rows.Scan(&tableName, &columnName, &columnType)
		if err != nil {
			continue
		}

		shapeName := tableName

		shapeDef, ok := s[shapeName]
		if !ok {
			shapeDef = &pipeline.ShapeDefinition{
				Name: shapeName,
			}
			s[shapeName] = shapeDef
		}

		shapeDef.Properties = append(shapeDef.Properties, pipeline.PropertyDefinition{
			Name: columnName,
			Type: convertSQLType(columnType),
		})
	}

	for _, sd := range s {
		defs = append(defs, *sd)
	}

	// Sort the shapes by Name
	sort.Sort(pipeline.SortShapesByName(defs))

	return defs, nil
}

func buildConnectionString(settings map[string]interface{}, timeout int) (string, error) {
	mr := utils.NewMapReader(settings)
	server, ok := mr.ReadString("server")
	if !ok {
		return "", errors.New("server cannot be null or empty")
	}
	user, ok := mr.ReadString("user")
	if !ok {
		return "", errors.New("user cannot be null or empty")
	}
	passwd, ok := mr.ReadString("password")
	if !ok {
		return "", errors.New("password cannot be null or empty")
	}
	port, ok := mr.ReadString("port")
	if !ok {
		return "", errors.New("port cannot be null or empty")
	}
	sid, ok := mr.ReadString("service_id")
	if !ok {
		return "", errors.New("service id cannot be null or empty")
	}

	cs := fmt.Sprintf("%s/%s@%s:%s/%s", user, passwd, server, port, sid)

	return cs, nil
}

func convertSQLType(t string) string {
	switch t {
	case "datetime":
	case "date":
	case "time":
	case "smalldatetime":
		return "date"
	case "bigint":
	case "int":
	case "smallint":
	case "tinyint":
		return "integer"
	case "decimal":
	case "float":
	case "money":
	case "smallmoney":
		return "float"
	case "bit":
		return "bool"
	}

	return "string"
}
