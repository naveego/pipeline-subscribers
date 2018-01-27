package main

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/api/utils"
	"github.com/naveego/navigator-go/subscribers/protocol"
)

type mssqlSubscriber struct {
	db             *sql.DB // The connection to the database
	tx             *sql.Tx // The current transaction
	count          int
	cmdType        string
	mappings       []pipeline.ShapeMapping
	shapes         pipeline.ShapeDefinitions
	ensuredSchemas []string // An array of schema names that have already been ensured by this subscriber
}

func (s *mssqlSubscriber) Init(request protocol.InitRequest) (protocol.InitResponse, error) {
	var resp protocol.InitResponse

	// Init may be called multiple times, so we need to close an Open
	// connection from a previous call
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}

	connString, err := buildConnectionString(request.Settings, 10)
	if err != nil {
		return resp, fmt.Errorf("could not connect to server: %v", err)
	}
	db, err := sql.Open("mssql", connString)
	if err != nil {
		return resp, fmt.Errorf("could not connect to server: %v", err)
	}

	sReq := protocol.DiscoverShapesRequest{
		Settings: request.Settings,
	}
	sResp, err := s.DiscoverShapes(sReq)
	if err != nil {
		return resp, fmt.Errorf("could not get shapes: %v", err)
	}

	mr := utils.NewMapReader(request.Settings)
	cmdType, _ := mr.ReadString("command_type")

	s.shapes = sResp.Shapes
	s.cmdType = cmdType
	s.db = db
	s.mappings = request.Mappings
	return resp, nil
}

func (s *mssqlSubscriber) TestConnection(request protocol.TestConnectionRequest) (protocol.TestConnectionResponse, error) {
	resp := protocol.TestConnectionResponse{}

	connString, err := buildConnectionString(request.Settings, 10)
	if err != nil {
		resp.Message = err.Error()
		return resp, err
	}
	conn, err := sql.Open("mssql", connString)
	if err != nil {
		resp.Message = err.Error()
		return resp, err
	}
	defer conn.Close()

	stmt, err := conn.Prepare("select 1")
	if err != nil {
		resp.Message = err.Error()
		return resp, err
	}
	defer stmt.Close()

	resp.Success = true
	resp.Message = "Connected Successfully"
	return resp, nil
}

func (s *mssqlSubscriber) DiscoverShapes(request protocol.DiscoverShapesRequest) (protocol.DiscoverShapesResponse, error) {
	resp := protocol.DiscoverShapesResponse{}

	mr := utils.NewMapReader(request.Settings)
	cmdType, _ := mr.ReadString("command_type")

	var shapes pipeline.ShapeDefinitions
	var err error

	if cmdType == "stored procedure" {
		shapes, err = getSPShapes(request.Settings)
	} else {
		shapes, err = getTableShapes(request.Settings)
	}

	resp.Shapes = shapes
	return resp, err
}

func (s *mssqlSubscriber) ReceiveDataPoint(request protocol.ReceiveShapeRequest) (protocol.ReceiveShapeResponse, error) {

	resp := protocol.ReceiveShapeResponse{}

	var shape pipeline.ShapeDefinition
	for _, x := range s.shapes {
		if x.Name == request.ShapeName {
			shape = x
		}
	}

	if shape.Name == "" {
		resp.Message = fmt.Sprintf("Could not find shape with name: %s", request.ShapeName)
		logrus.Error(resp.Message)
		return resp, nil
	}

	logrus.Debugf("Data Point: %v", request.DataPoint)

	var err error
	if s.cmdType == "stored procedure" {
		err = s.receiveShapeToSP(shape, request.DataPoint)
	} else {
		err = s.receiveShapeToTable(shape, request.DataPoint)
	}

	if err != nil {
		logrus.Error("Error receiving shape: ", err)
		resp.Success = false
		resp.Message = err.Error()
	} else {
		resp.Success = true
		resp.Message = "Received"
	}

	return resp, err
}

func (s *mssqlSubscriber) Dispose(request protocol.DisposeRequest) (protocol.DisposeResponse, error) {
	if s.db != nil {
		err := s.db.Close()
		s.db = nil
		if err != nil {
			return protocol.DisposeResponse{}, err
		}
	}

	return protocol.DisposeResponse{}, nil
}

func (s *mssqlSubscriber) receiveShapeToTable(shape pipeline.ShapeDefinition, dataPoint pipeline.DataPoint) error {
	schemaName := "dbo"
	tableName := shape.Name

	if strings.Contains(shape.Name, "__") {
		idx := strings.Index(shape.Name, "__")
		schemaName = tableName[:idx]
		tableName = tableName[idx+2:]
	}

	valCount := len(s.mappings)
	vals := make([]interface{}, valCount)
	for i := 0; i < valCount; i++ {
		vals[i] = new(interface{})
	}

	colNames := []string{}
	params := []string{}
	index := 1
	for _, m := range s.mappings {
		p := fmt.Sprintf("?%d", index)
		params = append(params, p)
		colNames = append(colNames, m.To)

		if v, ok := dataPoint.Data[m.From]; ok {
			vals[index-1] = v
		}

		index++
	}

	colNameStr := strings.Join(colNames, ",")
	paramsStr := strings.Join(params, ",")
	cmd := fmt.Sprintf("INSERT INTO [%s].[%s] (%s) VALUES (%s)", schemaName, tableName, colNameStr, paramsStr)

	logrus.Debugf("QUERY: %s", cmd)
	_, e := s.db.Exec(cmd, vals...)
	if e != nil {
		logrus.Errorf("Error executing query: %s %s", cmd, e)
		return e
	}

	return nil
}

func (s *mssqlSubscriber) receiveShapeToSP(shape pipeline.ShapeDefinition, dataPoint pipeline.DataPoint) error {
	schemaName := "dbo"
	spName := shape.Name

	if strings.Contains(shape.Name, "__") {
		idx := strings.Index(shape.Name, "__")
		schemaName = spName[:idx]
		spName = spName[idx+2:]
	}

	valCount := len(s.mappings)
	vals := make([]interface{}, valCount)
	for i := 0; i < valCount; i++ {
		vals[i] = new(interface{})
	}

	params := []string{}
	index := 1
	for _, m := range s.mappings {
		p := fmt.Sprintf(" %s = ?%d", m.To, index)
		params = append(params, p)

		if v, ok := dataPoint.Data[m.From]; ok {
			vals[index-1] = v
		}

		index++
	}

	paramsStr := strings.Join(params, ",")
	cmd := fmt.Sprintf("EXEC [%s].[%s] %s", schemaName, spName, paramsStr)
	_, e := s.db.Exec(cmd, vals...)
	if e != nil {
		return e
	}

	return nil
}

func getSPShapes(settings map[string]interface{}) (pipeline.ShapeDefinitions, error) {
	q := `select s.Name, o.Name, c.Name, ty.name from
			sys.procedures o
			INNER JOIN sys.schemas s ON (o.schema_id = s.schema_id)
			INNER JOIN sys.parameters c ON (o.object_id = c.object_id)
			INNER JOIN sys.types ty ON (c.user_type_id = ty.user_type_id)
			WHERE c.is_output = 0
			ORDER BY s.Name, o.Name, c.parameter_id`

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
	conn, err := sql.Open("mssql", connString)
	if err != nil {
		return defs, err
	}
	defer conn.Close()

	rows, err := conn.Query(query)

	if err != nil {
		return defs, err
	}
	defer rows.Close()

	var schemaName string
	var tableName string
	var columnName string
	var columnType string

	s := map[string]*pipeline.ShapeDefinition{}

	for rows.Next() {
		err = rows.Scan(&schemaName, &tableName, &columnName, &columnType)
		if err != nil {
			continue
		}

		shapeName := tableName
		if schemaName != "dbo" {
			shapeName = fmt.Sprintf("%s__%s", schemaName, tableName)
		}

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
	db, ok := mr.ReadString("database")
	if !ok {
		return "", errors.New("database cannot be null or empty")
	}
	auth, ok := mr.ReadString("auth")
	if !ok {
		return "", errors.New("auth type must be provided")
	}

	connStr := []string{
		"server=" + server,
		"database=" + db,
		"connection timeout=10",
	}

	if auth == "sql" {
		username, _ := mr.ReadString("username")
		pwd, _ := mr.ReadString("password")
		connStr = append(connStr, "user id="+username)
		connStr = append(connStr, "password="+pwd)
	}

	return strings.Join(connStr, ";"), nil
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
