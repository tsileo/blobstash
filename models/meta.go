package models

import (
	"github.com/garyburd/redigo/redis"
	"errors"
)

type Meta struct {
	Name string `redis:"name"`
	Type string `redis:"type"`
	Size int	`redis:"size"`
	Hash string `redis:"-"`
}

func NewMetaFromDB(pool *redis.Pool, key string) (m *Meta, err error) {
	m = &Meta{}
	con := pool.Get()
	defer con.Close()
	reply, err := redis.Values(con.Do("HGETALL", key))
	if err != nil {
		return
	}
	err = redis.ScanStruct(reply, m)
	m.Hash = key
	return
}

func GetAllMeta(pool *redis.Pool) (metas []*Meta, err error) {
	
	return
}

func NewMeta() *Meta {
	meta := &Meta{}
	return meta
}

func (m *Meta) Save(pool *redis.Pool) error {
	con := pool.Get()
	defer con.Close()
	if m.Hash == "" {
		return errors.New("Meta error: hash not set")
	}
	_, err := con.Do("HMSET", m.Hash, "name", m.Name, "type", m.Type, "size", m.Size)
	return err
}

func (m *Meta) IsFile() bool {
	if m.Type == "file" {
		return true
	}
	return false
}

func (m *Meta) IsDir() bool {
	if m.Type == "dir" {
		return true
	}
	return false
}
