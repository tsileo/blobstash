package models

import (
	"github.com/garyburd/redigo/redis"
	_ "strings"
	"fmt"
	"time"
	"strconv"
	"crypto/sha1"
)

type Backup struct {
	Name string `redis:"name"`
	Type string `redis:"type"`
	Ref string `redis:"ref"`
	Ts int64 `redis:"ts"`
	Hash string `redis:"-"`
}

func NewBackup(bname, btype, bref string) (f *Backup) {
	return &Backup{Name: bname, Type: btype, Ref: bref, Ts: time.Now().UTC().Unix()}
}

func NewBackupFromDB(pool *redis.Pool, key string) (f *Backup, err error) {
	f = &Backup{}
	con := pool.Get()
	defer con.Close()
	reply, err := redis.Values(con.Do("HGETALL", fmt.Sprintf("backup:%v", key)))
	if err != nil {
		return
	}
	err = redis.ScanStruct(reply, f)
	f.Hash = backupHashkey(f.Name, f.Ts)
	return
}

func backupHashkey(name string, ts int64) string {
	hash := sha1.New()
	hash.Write([]byte(name))
	hash.Write([]byte(strconv.Itoa(int(ts))))
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func (f *Backup) Save(pool *redis.Pool) (string, error) {
	con := pool.Get()
	defer con.Close()
	f.Hash = backupHashkey(f.Name, f.Ts)
	// TODO(tsileo) replace with a HMSET
	rkey := fmt.Sprintf("backup:%v", f.Hash)
	_, err := con.Do("HSET", rkey, "name", f.Name)
	_, err = con.Do("HSET", rkey, "type", f.Type)
	_, err = con.Do("HSET", rkey, "ref", f.Ref)
	_, err = con.Do("HSET", rkey, "ts", f.Ts)
	return f.Hash, err
}


