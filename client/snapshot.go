package client

import (
	"crypto/sha1"
	"fmt"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Snapshot struct {
	Type string `redis:"type"`
	Ref  string `redis:"ref"`
	Ts   int64  `redis:"ts"`
	Hostname string `redis:"hostname"`
	Description string `redis:"description"`
	Path string `redis:"path"`
	Hash string `redis:"-"`
}

func NewSnapshot(bhostname, bpath, btype, bref string) (s *Snapshot) {
	return &Snapshot{Path: bpath,
		Type: btype,
		Ref: bref,
		Ts: time.Now().UTC().Unix(),
		Hostname: bhostname}
}

func NewSnapshotFromDB(con redis.Conn, key string) (s *Snapshot, err error) {
	s = &Snapshot{}
	reply, err := redis.Values(con.Do("HGETALL", key))
	if err != nil {
		return
	}
	err = redis.ScanStruct(reply, s)
	s.computeHash()
	return
}

// computeHash compute and set the snapshot hash
// computed as follow:
// SHA1(hostname + path + timestamp)
func (s *Snapshot) computeHash() {
	hash := sha1.New()
	hash.Write([]byte(s.Hostname))
	hash.Write([]byte(s.Path))
	hash.Write([]byte(strconv.Itoa(int(s.Ts))))
	s.Hash = fmt.Sprintf("%x", hash.Sum(nil))
}

// snapshotKey compute the snapshot key
// computed as follow:
// SHA1(hostname + path)
func backupKey(hostname, path string) string {
	hash := sha1.New()
	hash.Write([]byte(hostname))
	hash.Write([]byte(path))
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// Save the backup to DB
func (s *Snapshot) Save(con redis.Conn) (error) {
	s.computeHash()
	_, err := con.Do("HMSET", s.Hash,
					"path", s.Path,
					"type", s.Type,
					"ref", s.Ref,
					"ts", s.Ts,
					"hostname", s.Hostname)
	if err != nil {
		return err
	}
	// Set/update the latest meta for this filename (snapshot)
	snapKey := backupKey(s.Hostname, s.Path)
	// Index the host
	if _, err := con.Do("SADD", "_hosts", s.Hostname); err != nil {
		return err
	}
	// Index the snapshot
	if _, err := con.Do("SADD", fmt.Sprintf("_backups:%v", s.Hostname), snapKey); err != nil {
		return err
	}
	if _, err := con.Do("LADD", snapKey, int(s.Ts), s.Hash); err != nil {
		return err
	}
	if _, err := con.Do("SET", s.Hash, s.Hostname); err != nil {
		return err
	}
	return nil
}

// Meta fetch the associated Meta
func (s *Snapshot) Meta(con redis.Conn) (m *Meta, err error) {
	return NewMetaFromDB(con, s.Ref)
}
