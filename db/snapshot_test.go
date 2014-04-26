package db

import (
	"testing"
	"time"
)

func TestDBSnapshot(t *testing.T) {
	db, err := New("test_db_snapshot")
	if err != nil {
		panic("db error")
	}
	defer func() {
		db.Close()
		db.Destroy()
	}()
	snap, snapExists := db.GetSnapshot("foo")
	if snapExists != false || snap != nil {
		t.Error("Snap shouldn't exists")
	}

	_, snapId := db.CreateSnapshot()
	_, snapExists = db.GetSnapshot(snapId)

	if !snapExists {
		t.Error("Error retrieving snapshot")
	}

	db.ReleaseSnapshot(snapId)
	
	snap, snapExists = db.GetSnapshot(snapId)
	if snapExists != false || snap != nil {
		t.Error("Snapshot should be released")
	}

	//TODO(tsileo) test the TTL
}

func TestDBSnapshotTTL(t *testing.T) {
	if testing.Short() {
        t.Skip("Skipping DB snapshot TTL test in short mode.")
    }
    db, err := New("test_db_snapshot")
	if err != nil {
		panic("db error")
	}
	defer func() {
		db.Close()
		db.Destroy()
	}()

	go db.SnapshotHandler()

	_, snapId := db.CreateSnapshot()
	db.UpdateSnapshotTTL(snapId, 0)
	time.Sleep(2 * time.Second)
	_, snapExists := db.GetSnapshot(snapId)
	if snapExists {
		t.Errorf("Snapshot %v should have been deleted", snapId)
	}

}
