package client

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/garyburd/redigo/redis"
)

var (
	ErrMetaAlreadyExists = errors.New("datadb: meta already exists")
)

var metaPool = sync.Pool{
	New: func() interface{} { return &Meta{} },
}

type Meta struct {
	Name string `redis:"name"`
	Type string `redis:"type"`
	Size int    `redis:"size"`
	Mode uint32 `redis:"mode"`
	ModTime string `redis:"mtime"`
	Ref  string `redis:"ref"`
	Hash string `redis:"-"`
}

func (m *Meta) free() {
	m.Name = ""
	m.Type = ""
	m.Size = 0
	m.Mode = 0
	m.ModTime = ""
	m.Ref = ""
	m.Hash = ""
	metaPool.Put(m)
}

func (m *Meta) metaKey() string {
	sha := sha1.New()
	sha.Write([]byte(m.Name))
	sha.Write([]byte(m.Type))
	sha.Write([]byte(strconv.Itoa(int(m.Size))))
	sha.Write([]byte(strconv.Itoa(int(m.Mode))))
	sha.Write([]byte(m.ModTime))
	sha.Write([]byte(m.Ref))
	return fmt.Sprintf("%x", sha.Sum(nil))
}

func NewMetaFromDB(con redis.Conn, key string) (m *Meta, err error) {
	m = metaPool.Get().(*Meta)
	reply, err := redis.Values(con.Do("HGETALL", key))
	if err != nil {
		return
	}
	err = redis.ScanStruct(reply, m)
	m.Hash = key
	return
}

//func GetAllMeta(pool *redis.Pool) (metas []*Meta, err error) {
//	return
//}

func NewMeta() *Meta {
	return metaPool.Get().(*Meta)
}

// Save the meta
func (m *Meta) Save(con redis.Conn) error {
	m.Hash = m.metaKey()
	cnt, err := redis.Int(con.Do("HLEN", m.Hash))
	if err != nil {
		return fmt.Errorf("error HLEN: %v", err)
	}
	if cnt != 0 {
		return nil
	}
	if _, err := con.Do("HMSET", m.Hash,
		"name", m.Name,
		"type", m.Type,
		"size", m.Size,
		"mtime", m.ModTime,
		"mode", m.Mode,
		"ref", m.Ref); err != nil {
		return fmt.Errorf("error HMSET: %v", m, err)
	}
	return err
}

// Save the meta
func (m *Meta) SaveToBuffer(con redis.Conn, rb *ReqBuffer) error {
	m.Hash = m.metaKey()
	cnt, err := redis.Int(con.Do("HLEN", m.Hash))
	if err != nil {
		return fmt.Errorf("error HLEN: %v", err)
	}
	if cnt != 0 {
		return nil
	}
	rb.Add("hmset", m.Hash, []string{
		"name", m.Name,
		"type", m.Type,
		"size", fmt.Sprintf("%v", m.Size),
		"mtime", m.ModTime,
		"mode", fmt.Sprintf("%v", m.Mode),
		"ref", m.Ref})
	return nil
}

func (m *Meta) ComputeHash() {
	m.Hash = m.metaKey()
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

type MetaFetcher interface {
	Get(string) interface{}
}

func (client *Client) MetaFromDB(key string) (*Meta, error) {
	con := client.Conn()
	defer con.Close()
	return NewMetaFromDB(con, key)
}

// Used by the LRU to fetch the Meta for the given dir/file
func (client *Client) FetchMeta(con redis.Conn, key string) interface{} {
	meta, err := NewMetaFromDB(con, key)
	if err != nil {
		panic(fmt.Sprintf("Error FetchMeta key:%v", key))
	}
	return meta
}
