package models

import (
	"github.com/garyburd/redigo/redis"
	"errors"
	"log"
)

type Meta struct {
	Name string `redis:"name"`
	Type string `redis:"type"`
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
	return
}

func (m *Meta) Save(pool *redis.Pool) error {
	con := pool.Get()
	defer con.Close()
	if m.Hash == "" {
		return errors.New("Meta error: hash not set")
	}
	// TODO(tsileo) replace with a HMSET
	_, err := con.Do("HSET", m.Hash, "name", m.Name)
	_, err = con.Do("HSET", m.Hash, "type", m.Type)
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

func (m *Meta) PutFile(path string) (h string, err error) {
	wr, err := RawPutFile(path)
	if err != nil {
		return
	}
	log.Printf("%+v", wr)
	h = wr.Hash
	m.Hash = h
	m.Type = "file"
	return 
}

func (m *Meta) GetFile(key, path string) error {
	rw, err := FileReader(key, path)
	log.Printf("%+v", rw)
	return err
}
