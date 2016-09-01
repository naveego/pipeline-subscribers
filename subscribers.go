package subscribers

import (
	"github.com/naveego/pipeline-api/subscriber"
	"github.com/naveego/pipeline-subscribers/sql/mssql"
	"github.com/naveego/pipeline-subscribers/web/wellcast"
)

func init() {
	subscriber.RegisterFactory("mssql", func() subscriber.Subscriber { return mssql.Subscriber{} })
	subscriber.RegisterFactory("wellcast", func() subscriber.Subscriber { return wellcast.Subscriber{} })
}
