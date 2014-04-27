package models

import (
	"github.com/garyburd/redigo/redis"
	"log"
)

type Backup struct {
	Name string `redis:"name"`
	Type string `redis:"type"`
	Data string `redis:"data"`
	Ts int64 `redis:"ts"`
}

func NewFromDB(pool *redis.Pool, key string) (f *Backup, err error) {
	f = &Backup{}
	con := pool.Get()
	defer con.Close()
	reply, err := redis.Values(con.Do("HGETALL", key))
	log.Printf("%+v", reply)
	if err != nil {
		return
	}
	err = redis.ScanStruct(reply, f)
	return
}

func (f *Backup) Save(pool *redis.Pool, key string) error {
	con := pool.Get()
	defer con.Close()
	// TODO(tsileo) replace with a HMSET
	_, err := con.Do("HSET", key, "name", f.Name)
	_, err = con.Do("HSET", key, "type", f.Type)
	_, err = con.Do("HSET", key, "data", f.Data)
	_, err = con.Do("HSET", key, "ts", f.Ts)
	return err
}
