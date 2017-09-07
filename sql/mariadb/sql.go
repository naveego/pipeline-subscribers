package main

import (
	"bytes"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/naveego/navigator-go/subscribers/protocol"

	"github.com/naveego/pipeline-subscribers/shapeutils"
)

const createTemplateText = `CREATE TABLE IF NOT EXISTS {{tick .Name}} ({{range .Columns}}
	{{tick .Name}} {{.SqlType}} {{if .IsKey}}NOT {{end}}NULL,{{end}}{{if gt (len .Keys) 0}}
	PRIMARY KEY ({{jointick .Keys}}){{end}}
)`

const alterTemplateText = `ALTER TABLE {{tick .Name}}{{range $i, $e := .Columns}}
	{{if $i}},{{end}}ADD COLUMN IF NOT EXISTS {{tick $e.Name}} {{$e.SqlType}} {{if $e.IsKey}}NOT {{end}}NULL{{end}}{{if gt (len .Keys) 0}}
	,DROP PRIMARY KEY
	,ADD PRIMARY KEY ({{jointick .Keys}}){{end}};`

const upsertTemplateText = `INSERT INTO {{tick .Name}} ({{range $i, $e := .Columns}}{{if $i}}, {{end}}{{tick $e.Name}}{{end}})
	VALUES ({{range $i, $e := .Columns}}{{if $i}}, {{end}}?{{end}})
	ON DUPLICATE KEY UPDATE{{range $i, $e := .Columns}}{{if not $e.IsKey}}
		{{if $i}},{{end}}{{tick $e.Name}} = VALUES({{tick $e.Name}}){{end}}{{end}};`

var (
	alterTemplate  *template.Template
	createTemplate *template.Template
	upsertTemplate *template.Template
)

func init() {
	funcs := template.FuncMap{
		"tick":     func(item string) string { return "`" + item + "`" },
		"join":     func(items []string) string { return strings.Join(items, "`, `") },
		"jointick": func(items []string) string { return "`" + strings.Join(items, "`, `") + "`" },
	}
	alterTemplate = template.Must(template.New("alter").
		Funcs(funcs).
		Parse(alterTemplateText))

	createTemplate = template.Must(template.New("create").
		Funcs(funcs).
		Parse(createTemplateText))

	upsertTemplate = template.Must(template.New("upsert").
		Funcs(funcs).
		Parse(upsertTemplateText))

}

func createShapeChangeSQL(shapeInfo shapeutils.ShapeInfo) (string, error) {

	var (
		err error
		w   = &bytes.Buffer{}
	)

	model := sqlTableModel{
		Name: escapeString(shapeInfo.ShapeDef.Name),
		Keys: shapeInfo.ShapeDef.Keys,
	}
	for n, t := range shapeInfo.NewProperties {
		columnModel := sqlColumnModel{
			Name:    escapeString(n),
			SqlType: convertToSQLType(t),
		}
		for _, k := range shapeInfo.ShapeDef.Keys {
			if k == n {
				columnModel.IsKey = true
			}
		}

		model.Columns = append(model.Columns, columnModel)
	}

	sort.Sort(model.Columns)

	if shapeInfo.IsNew {
		err = createTemplate.Execute(w, model)
	} else {
		err = alterTemplate.Execute(w, model)
	}

	command := w.String()

	return command, err
}

type sqlTableModel struct {
	Name    string
	Columns sqlColumns
	Keys    []string
}

type sqlColumns []sqlColumnModel

type sqlColumnModel struct {
	Name    string
	SqlType string
	IsKey   bool
}

func (s sqlColumns) Len() int {
	return len(s)
}
func (s sqlColumns) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s sqlColumns) Less(i, j int) bool {
	return strings.Compare(s[i].Name, s[j].Name) < 0
}

func createUpsertSQL(request protocol.ReceiveShapeRequest) (sql string, params []interface{}, err error) {

	model := sqlTableModel{
		Name: escapeString(request.Shape.Name),
	}
	for _, p := range request.Shape.Properties {
		columnModel := sqlColumnModel{
			Name:    escapeString(p.Name),
			SqlType: convertToSQLType(p.Type),
		}
		for _, k := range request.Shape.Keys {
			if k == p.Name {
				columnModel.IsKey = true
			}
		}

		model.Columns = append(model.Columns, columnModel)
	}

	// Make sure we have the columns in a known order, for consistency
	sort.Sort(model.Columns)

	// Render the SQL
	w := &bytes.Buffer{}
	err = upsertTemplate.Execute(w, model)
	if err != nil {
		return
	}

	sql = w.String()

	// Populate the parameter list with values from the datapoint,
	// in the column order.
	for _, c := range model.Columns {
		value := request.DataPoint.Data[c.Name]
		params = append(params, value)
	}

	return
}

func convertToSQLType(t string) string {
	switch t {
	case "date":
		return "DATETIME"
	case "integer":
		return "INT(10)"
	case "float":
		return "FLOAT"
	case "bool":
		return "BIT"
	}

	return "VARCHAR(1000)"
}

func convertFromSQLType(t string) string {

	text := strings.ToLower(strings.Split(t, "(")[0])

	switch text {
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

var sqlCleaner = regexp.MustCompile(`[^A-z0-9_\-\. ]|` + "`")

func escapeArgs(args ...string) []interface{} {
	safeArgs := make([]interface{}, len(args))
	for i, a := range args {
		safeArgs[i] = escapeString(a)
	}
	return safeArgs
}

func escapeString(arg string) string {
	return sqlCleaner.ReplaceAllString(arg, "")
}
