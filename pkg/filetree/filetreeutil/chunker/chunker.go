/*

Package chunker implements a chunker based on a rolling Rabin fingerprint to determine block boundaries.

Implementation similar to https://github.com/cschwede/python-rabin-fingerprint

*/
package chunker

type Chunker struct {
	window  []uint64
	pos     int
	prevPos int

	WindowSize uint64
	Prime      uint64

	Fingerprint uint64

	ChunkMinSize uint64
	ChunkAvgSize uint64
	ChunkMaxSize uint64

	BlockSize uint64
}

// Same window size as LBFS 48
var windowSize = 64
var prime = uint64(31)

var cache [256]uint64

func init() {
	// calculates result = Prime ^ WindowSize first
	result := uint64(1)
	for i := 1; i < windowSize; i++ {
		result *= prime
	}
	// caches the result for all 256 bytes
	for i := uint64(0); i < 256; i++ {
		cache[i] = i * result
	}

}

// TODO build the cache in init

func New() *Chunker {
	return &Chunker{
		window:       make([]uint64, windowSize),
		pos:          0,
		prevPos:      windowSize - 1,
		WindowSize:   uint64(windowSize),
		ChunkMinSize: 256 * 1024,
		ChunkAvgSize: 1024 * 1024,
		ChunkMaxSize: 4 * 1024 * 1024,
	}
}

func (chunker *Chunker) Write(data []byte) (n int, err error) {
	for _, c := range data {
		chunker.WriteByte(c)
	}
	return len(data), nil
}

func (chunker *Chunker) WriteByte(c byte) error {
	ch := uint64(c)
	chunker.Fingerprint *= prime
	chunker.Fingerprint += ch
	//fmt.Printf("chunker=%+v/%+v/%+v\n", chunker.pos, len(chunker.window), chunker.prevPos)
	chunker.Fingerprint -= cache[chunker.window[chunker.prevPos]]

	chunker.window[chunker.pos] = ch
	chunker.prevPos = chunker.pos
	chunker.pos = (chunker.pos + 1) % int(chunker.WindowSize)
	chunker.BlockSize++
	return nil
}

func (chunker *Chunker) OnSplit() bool {
	if chunker.BlockSize > chunker.ChunkMinSize {
		if chunker.Fingerprint%chunker.ChunkAvgSize == 1 || chunker.BlockSize >= chunker.ChunkMaxSize {
			return true
		}
	}
	return false
}

func (chunker *Chunker) Reset() {
	chunker.BlockSize = 0
}
