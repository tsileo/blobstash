package db

import (
	"time"
	"github.com/jmhodges/levigo"
	"log"
)

// Infinite loop that check snapshots TTL and release them if expired
func (db *DB) SnapshotHandler() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for t := range ticker.C {
			now := t.UTC().Unix()
			for snapId, snapTTL := range db.snapshotsTTL {
				if now >= snapTTL {
					log.Printf("SnapshotHandler: releasing timed-out snapshot %v\n", snapId)
					db.ReleaseSnapshot(snapId)
				}
			}
		}
	}()
}

// Create a new LevelDB snapshot and return its newly generated ID
func (db *DB) CreateSnapshot() string {
	db.snapMutex.Lock()
	defer db.snapMutex.Unlock()
	snap := db.ldb.NewSnapshot()
	snapId := NewId()
	db.snapshots[snapId] = snap
	db.snapshotsTTL[snapId] = time.Now().UTC().Unix() + SnapshotTTL
	return snapId
}

// Retrieve the given LevelDB snapshot
func (db *DB) GetSnapshot(snapId string) (snap *levigo.Snapshot, snapExists bool) {
	db.snapMutex.Lock()
	defer db.snapMutex.Unlock()
	snap, snapExists = db.snapshots[snapId]
	return
}

// Update the TTL of the given snapshot
func (db *DB) UpdateSnapshotTTL(snapId string, ttl int) {
	db.snapMutex.Lock()
	defer db.snapMutex.Unlock()
	_, snapExists := db.snapshotsTTL[snapId]
	if snapExists {
		db.snapshotsTTL[snapId] = time.Now().UTC().Unix() + int64(ttl)
	}
	return
}

// Release the LevelDB snapshot
func (db *DB) ReleaseSnapshot(snapId string) {
	snap, snapExists := db.GetSnapshot(snapId)
	if snapExists {
		db.ldb.ReleaseSnapshot(snap)
		db.snapMutex.Lock()
		defer db.snapMutex.Unlock()
		delete(db.snapshots, snapId)
		delete(db.snapshotsTTL, snapId)
	}
	return
}
