package client

import (
	"fmt"
	"log"

	"github.com/garyburd/redigo/redis"
)

type Backup struct {
	client *Client
	SnapKey string // Hold the backup key
	snapshots []*IndexSnapshot
}

func NewBackup(client *Client, snapKey string) (*Backup, error) {
	backup := &Backup{client: client, SnapKey: snapKey}
	return backup, nil
}

type IndexSnapshot struct {
	Index int
	Snapshot  *Snapshot
}

func (b *Backup) Snapshots() ([]*IndexSnapshot, error) {
	b.snapshots = []*IndexSnapshot{}
	var indexValueList []struct {
		Index int
		Value string
	}
	con := b.client.Pool.Get()
	defer con.Close()
	values, err := redis.Values(con.Do("LITER", b.SnapKey, "WITH", "INDEX"))
	if err != nil {
		return nil, err
	}
	redis.ScanSlice(values, &indexValueList)
	for _, iv := range indexValueList {
		snap, berr := NewSnapshotFromDB(b.client.Pool, iv.Value)
		if berr != nil {
			return nil, berr
		}
		//meta := client.Metas.Get(snap.Ref).(*Meta)
		b.snapshots = append(b.snapshots, &IndexSnapshot{iv.Index, snap})
	}
	return b.snapshots, nil
}

// GetAt fetch the meta that is grater than the given timestamp.
func (b *Backup) GetAt(ts int64) (*Snapshot, error) {
	snapshots, err := b.Snapshots()
	if err != nil {
		return nil, err
	}
	for i, snapIndex := range snapshots {
		if snapIndex.Index > int(ts) {
			if i != 0 {
				return snapshots[i-1].Snapshot, nil
			} else {
				// FIX ME buggy
				return snapIndex.Snapshot, nil
			}
		}
	}
	return nil, fmt.Errorf("no meta match the timestamp %v", ts)
}

// Last fetch the latest snapshot.
func (b *Backup) Last() (*Snapshot, error) {
	snapshots, err := b.Snapshots()
	if err != nil {
		return nil, err
	}
	return snapshots[len(snapshots)-1].Snapshot, nil
}

// Hosts returns the list of hostname
func (client *Client) Hosts() ([]string, error) {
	con := client.Pool.Get()
	defer con.Close()
	return redis.Strings(con.Do("SMEMBERS", "_hosts"))
}

// Backups return all backups for the given host.
func (client *Client) Backups(host string) ([]*Backup, error) {
	con := client.Pool.Get()
	defer con.Close()
	keys, err := redis.Strings(con.Do("SMEMBERS", fmt.Sprintf("_backups:%v", host)))
	if err != nil {
		return nil, err
	}

	backups := []*Backup{}
	for _, key := range keys {
		backup, _ := NewBackup(client, key)
		log.Printf("backup: %+v", backup)
		backups = append(backups, backup)
	}
	return backups, err
}
