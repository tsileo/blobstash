package models

import (
	"github.com/garyburd/redigo/redis"
	"log"
)

// Returns a list of Backup, the latest for each filename/snapshot
func (client *Client) Latest() (backups []*Backup, err error) {
	//indexValueList
	var _ []struct {
	    Index int
	    Value string
	}
	con := client.Pool.Get()
	defer con.Close()
	keys, err := redis.Strings(con.Do("RANGE", "latest:", "latest:\xff", 0))
	log.Printf("keys:%v,err:%v", keys, err)
	for _, key := range keys {
		backup, berr := NewBackupFromDB(client.Pool, key)
		if berr != nil {
			return backups, berr
		}
		backups = append(backups, backup)
	}
	return

}

func (client *Client) SnapshotIter() (filenames []string, err error) {
	con := client.Pool.Get()
	defer con.Close()
	filenames, err = redis.Strings(con.Do("SMEMBERS", "filenames"))
	return
}

// TODO(tsileo) add a LRU for snapshots queries

type IndexMeta struct {
	Index int
	Meta *Meta
}

func (client *Client) Snapshots(filename string) (ivs []*IndexMeta, err error) {
	var indexValueList []struct {
	    Index int
	    Value string
	}
	con := client.Pool.Get()
	defer con.Close()
	values, err := redis.Values(con.Do("LITER", filename, "WITH", "INDEX"))
	if err != nil {
		return nil, err
	}
	redis.ScanSlice(values, &indexValueList)
	for _, iv := range indexValueList {
		meta, merr := NewMetaFromDB(client.Pool, iv.Value)
		if merr != nil {
			return nil, merr
		}
		ivs = append(ivs, &IndexMeta{iv.Index, meta})
	}
	return	
}
