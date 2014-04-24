package db

func (db *DB) GetBlobsCnt() uint32 {
	return db.getUint32(KeyType(BlobsCnt, Meta))
}

func (db *DB) IncrBlobsCnt(step int) {
	db.incrUint32(KeyType(BlobsCnt, Meta), step)
}

func (db *DB) GetBlobsSize() uint32 {
	return db.getUint32(KeyType(BlobsSize, Meta))
}

func (db *DB) IncrBlobsSize(step int) {
	db.incrUint32(KeyType(BlobsSize, Meta), step)
}
