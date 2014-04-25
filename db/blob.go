package db

func (db *DB) GetBlobsCnt() (uint32, error) {
	return db.getUint32(KeyType(BlobsCnt, Meta))
}

func (db *DB) IncrBlobsCnt(step int) error {
	return db.incrUint32(KeyType(BlobsCnt, Meta), step)
}

func (db *DB) GetBlobsSize() (uint32, error) {
	return db.getUint32(KeyType(BlobsSize, Meta))
}

func (db *DB) IncrBlobsSize(step int) error {
	return db.incrUint32(KeyType(BlobsSize, Meta), step)
}
