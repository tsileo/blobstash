package client

import (
	"github.com/garyburd/redigo/redis"
	"errors"
	"fmt"
	"crypto/sha1"
)

var (
	ErrMetaAlreadyExists = errors.New("datadb: meta already exists")
)

type Meta struct {
	Name string `redis:"name"`
	Type string `redis:"type"`
	Size int	`redis:"size"`
	Ref string	`redis:"ref"`
	Hash string `redis:"-"`
}

func metaKey(filename, hash string) string {
	sha := sha1.New()
	sha.Write([]byte(filename))
	sha.Write([]byte(hash))
	return fmt.Sprintf("%x", sha.Sum(nil))
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

// Save the meta
func (m *Meta) Save(txID string, pool *redis.Pool) error {
	con := pool.Get()
	defer con.Close()
	if _, err := redis.String(con.Do("TXINIT", txID)); err != nil {
		return err
	}
	m.Hash = metaKey(m.Name, m.Ref)
	cnt, err := redis.Int(con.Do("HLEN", m.Hash))
	if err != nil {
		return err
	}
	if cnt != 0 {
		return nil
	}
	if _, err := con.Do("HMSET", m.Hash,
			"name", m.Name,
			"type", m.Type,
			"size", m.Size,
			"ref", m.Ref); err != nil {
		return err
	}

	_, err = con.Do("TXCOMMIT")
	return err
}

func (m *Meta) ComputeHash() {
	m.Hash = metaKey(m.Name, m.Ref)
	return
}

// IsFile returns true if the Meta is a file.
func (m *Meta) IsFile() bool {
	if m.Type == "file" {
		return true
	}
	return false
}

// IsDir returns true if the Meta is a directory.
func (m *Meta) IsDir() bool {
	if m.Type == "dir" {
		return true
	}
	return false
}

type MetaFetcher interface{
	Get(string) interface{}
}

func (client *Client) MetaFromDB(key string) (*Meta, error) {
	return NewMetaFromDB(client.Pool, key)
}

// Used by the LRU to fetch the Meta for the given dir/file
func (client *Client) FetchMeta(key string) interface{} {
	metas, err := client.MetaFromDB(key)
	if err != nil {
		panic(fmt.Sprintf("Error FetchMeta key:%v", key))
	}
	return metas
}
