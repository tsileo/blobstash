package ctx

import (
	"github.com/garyburd/redigo/redis"
)

// Ctx holds a request context
type Ctx struct {
	Namespace string
	MetaBlob  bool // Flag for blob upload
}

// Args cast the Ctx into Redis.Args
func (ctx *Ctx) Args() redis.Args {
	return redis.Args{}.Add(ctx.Namespace)
}

func (ctx *Ctx) Meta() *Ctx {
	return &Ctx{
		Namespace: ctx.Namespace,
		MetaBlob: true,
	}
}
