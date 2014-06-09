package client

import (
	"github.com/garyburd/redigo/redis"
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
	snapshots, err := client.SnapshotIter()
	for _, snap := range snapshots {
		// Get the latest backup for this backup/snapshot
		key, kerr := redis.String(con.Do("LLAST", snap, "0", "\xff", 0))
		if kerr != nil {
			return backups, kerr
		}
		backup, berr := NewBackupFromDB(client.Pool, key)
		if berr != nil {
			return backups, berr
		}
		backups = append(backups, backup)
	}
	return

}

// SnapshotIter returns a slice of every snapshots keys.
func (client *Client) SnapshotIter() (snapshots []string, err error) {
	con := client.Pool.Get()
	defer con.Close()
	snapshots, err = redis.Strings(con.Do("SMEMBERS", "snapshots"))
	return
}

// TODO(tsileo) add a LRU for snapshots queries

type IndexMeta struct {
	Index int
	Meta  *Meta
}

func (client *Client) Snapshots(snapKey string) (ivs []*IndexMeta, err error) {
	var indexValueList []struct {
		Index int
		Value string
	}
	con := client.Pool.Get()
	defer con.Close()
	values, err := redis.Values(con.Do("LITER", snapKey, "WITH", "INDEX"))
	if err != nil {
		return nil, err
	}
	redis.ScanSlice(values, &indexValueList)
	for _, iv := range indexValueList {
		backup, berr := NewBackupFromDB(client.Pool, iv.Value)
		if berr != nil {
			return nil, berr
		}
		meta := client.Metas.Get(backup.Ref).(*Meta)
		//meta, merr := backup.Meta(client.Pool)
		//if merr != nil {
		//	return nil, merr
		//}
		ivs = append(ivs, &IndexMeta{iv.Index, meta})
	}
	return
}

// GetAt fetch the backup ref that match the given snapshot key/timestamp.
func (client *Client) GetAt(snapKey string, ts int64) (string, error) {
	con := client.Pool.Get()
	defer con.Close()
	backup, err := redis.String(con.Do("LPREV", snapKey, ts))
	if err != nil {
		return "", err
	}
	if backup != "" {
		return redis.String(con.Do("HGET", backup, "ref"))
	}
	return "", nil
}
