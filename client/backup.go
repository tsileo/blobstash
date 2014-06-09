package client

import (
	"crypto/sha1"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
	_ "strings"
	"time"
)

type Backup struct {
	Type string `redis:"type"`
	Ref  string `redis:"ref"`
	Ts   int64  `redis:"ts"`
	Hostname string `redis:"hostname"`
	Description string `redis:"description"`
	Path string `redis:"path"`
	Hash string `redis:"-"`
}

func NewBackup(bhostname, bpath, btype, bref string) (f *Backup) {
	return &Backup{Path: bpath, Type: btype, Ref: bref,
		Ts: time.Now().UTC().Unix(),
		Hostname: bhostname}
}

func NewBackupFromDB(pool *redis.Pool, key string) (f *Backup, err error) {
	f = &Backup{}
	con := pool.Get()
	defer con.Close()
	reply, err := redis.Values(con.Do("HGETALL", key))
	if err != nil {
		return
	}
	err = redis.ScanStruct(reply, f)
	f.Hash = backupHashkey(f.Hostname, f.Path, f.Ts)
	return
}

func backupHashkey(hostname, name string, ts int64) string {
	hash := sha1.New()
	hash.Write([]byte(hostname))
	hash.Write([]byte(name))
	hash.Write([]byte(strconv.Itoa(int(ts))))
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// Save the backup to DB
func (f *Backup) Save(pool *redis.Pool) (string, error) {
	con := pool.Get()
	defer con.Close()
	f.Hash = backupHashkey(f.Hostname, f.Path, f.Ts)
	rkey := fmt.Sprintf("backup:%v", f.Hash)
	_, err := redis.String(con.Do("TXINIT"))
	if err != nil {
		return rkey, err
	}
	_, err = con.Do("HMSET", rkey,
					"path", f.Path,
					"type", f.Type,
					"ref", f.Ref,
					"ts", f.Ts,
					"hostname", f.Hostname)
	if err != nil {
		return rkey, err
	}
	// Set/update the latest meta for this filename (snapshot)
	_, err = con.Do("SADD", "filenames", f.Path)
	if err != nil {
		return rkey, err
	}
	_, err = con.Do("LADD", f.Path, int(f.Ts), rkey)
	if err != nil {
		return rkey, err
	}
	_, err = con.Do("TXCOMMIT")
	return rkey, err
}

// Fetch the associated Meta directly
func (b *Backup) Meta(pool *redis.Pool) (m *Meta, err error) {
	return NewMetaFromDB(pool, b.Ref)
}
