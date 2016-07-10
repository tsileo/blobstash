package blobsfile

import (
	"github.com/klauspost/reedsolomon"
	_ "github.com/tsileo/blobstash/pkg/logger"
)

type CorruptedBlob struct {
	Hash   string
	Offset int64
	Size   int64
}

type Chunk struct {
	Data         []byte
	Offset, Size int64
	Hash         string
	Corrupted    bool
}

func InvalidChunks(blobs []*CorruptedBlob, chunks []*Chunk) error {
	// TODO(tsileo): nilify the chunks invalidated by the corrupted blobs
	// take in account the fact that a blob can invalidate two chunks
	return nil
}

type reedSolomonEncoder struct {
	enc reedsolomon.Encoder
}

// New
func NewReedSolomonEncoder() (*reedSolomonEncoder, error) {
	enc, err := reedsolomon.New(16, 4)
	if err != nil {
		return nil, err
	}
	return &reedSolomonEncoder{enc}, nil
}

// Encode returns a slice containig the parity shards
func (rse *reedSolomonEncoder) Encode(data []byte) ([][]byte, error) {
	shards, err := rse.enc.Split(data)
	if err != nil {
		return nil, err
	}

	// TODO(tsileo): build a list of chunk? or take a list of shard?

	// Encode the parity set
	if err := rse.enc.Encode(shards); err != nil {
		return nil, err
	}

	return shards[16 : len(shards)-1], nil
}

// Reconstruct try to fix any error if possible, chunks is expected to be the data blocks,
// followed by the parity blocks
func (rse *reedSolomonEncoder) Reconstruct(chunks [][]byte) error {
	return rse.enc.Reconstruct(chunks)
}
