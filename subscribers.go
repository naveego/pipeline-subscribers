package subscribers

import (
	"github.com/naveego/pipeline-api/subscriber"
	"github.com/naveego/pipeline-subscribers/sql/mssql"
	"github.com/naveego/pipeline-subscribers/web/wellcast"
)

func init() {
	subscriber.RegisterFactory("mssql", mssql.NewSubscriber)
	subscriber.RegisterFactory("wellcast", wellcast.NewSubscriber)
}
