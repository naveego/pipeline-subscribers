package main

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	"github.com/naveego/api/pipeline/subscriber"
	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/navigator-go/subscribers/protocol"
	"github.com/naveego/pipeline-subscribers/shapeutils"

	_ "github.com/go-sql-driver/mysql"
)

type mariaSubscriber struct {
	db             *sql.DB // The connection to the database
	connectionInfo string
	knownShapes    map[string]pipeline.ShapeDefinition
}

type settings struct {
	DataSourceName string
}

func (h *mariaSubscriber) Init(request protocol.InitRequest) (protocol.InitResponse, error) {

	var (
		response = protocol.InitResponse{}
		err      error
	)

	err = h.connect(request.Settings)

	if err != nil {
		return response, err
	}

	response.Message = h.connectionInfo
	response.Success = true

	return response, nil
}

func (h *mariaSubscriber) TestConnection(request protocol.TestConnectionRequest) (protocol.TestConnectionResponse, error) {

	resp, err := h.Init(protocol.InitRequest{Settings: request.Settings})

	return protocol.TestConnectionResponse{
		Message: resp.Message,
		Success: resp.Success,
	}, err
}

func (h *mariaSubscriber) DiscoverShapes(request protocol.DiscoverShapesRequest) (protocol.DiscoverShapesResponse, error) {

	var (
		response = protocol.DiscoverShapesResponse{}
		err      error
	)

	err = h.connect(request.SubscriberInstance.Settings)

	if err != nil {
		return response, err
	}

	response.Shapes = make([]pipeline.ShapeDefinition, 0, len(h.knownShapes))
	for _, shape := range h.knownShapes {
		response.Shapes = append(response.Shapes, shape)
	}

	return response, err
}

func (h *mariaSubscriber) ReceiveDataPoint(request protocol.ReceiveShapeRequest) (protocol.ReceiveShapeResponse, error) {

	var (
		response = protocol.ReceiveShapeResponse{}
	)

	if h.db == nil {
		return response, errors.New("you must call Init before sending data points")
	}

	shapeInfo := shapeutils.GenerateShapeInfo(h.knownShapes, request.Shape)

	if shapeInfo.HasChanges() {

		sqlCommand, err := createShapeChangeSQL(shapeInfo)
		if err != nil {
			return response, err
		}

		_, err = h.db.Exec(sqlCommand)

		if err != nil {
			return response, err
		}

	}

	upsertCommand, upsertParameters, err := createUpsertSQL(request)
	if err != nil {
		return response, err
	}

	_, err = h.db.Exec(upsertCommand, upsertParameters...)

	return protocol.ReceiveShapeResponse{
		Success: true,
	}, nil
}

func (h *mariaSubscriber) Dispose(request protocol.DisposeRequest) (protocol.DisposeResponse, error) {

	if h.db == nil {
		return protocol.DisposeResponse{
			Success: true,
			Message: "Not initialized.",
		}, nil
	}

	err := h.db.Close()
	h.db = nil

	if err != nil {
		return protocol.DisposeResponse{
			Success: true,
			Message: "Error while closing connection.",
		}, err
	}

	return protocol.DisposeResponse{
		Success: true,
		Message: "Closed connection.",
	}, nil
}

func (h *mariaSubscriber) connect(settingsMap map[string]interface{}) error {

	// If we already connected, we shouldn't do anything.
	if h.db != nil {
		return nil
	}

	var (
		settings = &settings{}
		err      error
		version  string
		db       *sql.DB
	)

	err = mapstructure.Decode(settingsMap, settings)
	if err != nil {
		return fmt.Errorf("couldn't decode settings: %s", err)
	}

	if settings.DataSourceName == "" {
		return errors.New("settings didn't contain DataSourceName key")
	}

	db, err = sql.Open("mysql", settings.DataSourceName)

	if err != nil {
		return fmt.Errorf("couldn't open SQL connection: %s", err)
	}

	db.QueryRow("SELECT VERSION()").Scan(&version)

	if len(version) == 0 {
		return fmt.Errorf("couldn't get data from database server")
	}

	h.connectionInfo = fmt.Sprintf("Connected to: %s", version)
	h.db = db
	shapes, err := h.getKnownShapes()
	if err != nil {
		return err
	}

	h.knownShapes = make(map[string]pipeline.ShapeDefinition)

	for _, shape := range shapes {
		h.knownShapes[shape.Name] = shape
	}

	return nil
}

func (s *mariaSubscriber) receiveShapeToTable(ctx subscriber.Context, shape pipeline.ShapeDefinition, dataPoint pipeline.DataPoint) error {
	schemaName := "dbo"
	tableName := shape.Name

	if strings.Contains(shape.Name, "__") {
		idx := strings.Index(shape.Name, "__")
		schemaName = tableName[:idx]
		tableName = tableName[idx+2:]
	}

	valCount := len(ctx.Pipeline.Mappings)
	vals := make([]interface{}, valCount)
	for i := 0; i < valCount; i++ {
		vals[i] = new(interface{})
	}

	colNames := []string{}
	params := []string{}
	index := 1
	for _, m := range ctx.Pipeline.Mappings {
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
	logrus.Infof("Command: %s", cmd)
	_, e := s.db.Exec(cmd, vals...)
	if e != nil {
		return e
	}

	return nil
}

func (h *mariaSubscriber) getKnownShapes() (pipeline.ShapeDefinitions, error) {

	var (
		err        error
		rows       *sql.Rows
		tableNames []string
		shapes     pipeline.ShapeDefinitions
	)

	rows, err = h.db.Query("SHOW TABLES")
	if err != nil {
		return shapes, err
	}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return shapes, err
		}
		tableNames = append(tableNames, tableName)
	}

	for _, table := range tableNames {

		rows, err = h.db.Query(fmt.Sprintf("DESCRIBE `%s`", table))
		if err != nil {
			return shapes, err
		}
		shape := pipeline.ShapeDefinition{}

		for rows.Next() {
			var (
				field   string
				coltype string
				null    string
				key     string
				def     interface{}
				extra   string
			)
			err = rows.Scan(&field, &coltype, &null, &key, &def, &extra)
			if err != nil {
				return shapes, err
			}
			shape.Name = table
			if key == "PRI" {
				shape.Keys = append(shape.Keys, field)
			}
			property := pipeline.PropertyDefinition{
				Name: field,
				Type: convertFromSQLType(coltype),
			}

			shape.Properties = append(shape.Properties, property)
		}

		sort.Sort(pipeline.SortPropertyDefinitionsByName(shape.Properties))

		shapes = append(shapes, shape)
	}

	sort.Sort(pipeline.SortShapesByName(shapes))

	return shapes, nil
}
