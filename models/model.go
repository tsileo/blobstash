package models

import (
	"github.com/garyburd/redigo/redis"
)

type Model interface {
	Save(pool *redis.Pool, key string) error
}
