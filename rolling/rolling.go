// Package rolling implements a 32 bit rolling checksum similar to rsync's
// algorithm taken from https://github.com/rwcarlsen/gobup.
package rolling

const blobSize = 1 << 13

type RollingSum struct {
	a      uint16
	b      uint16
	window []byte
	size   int
	i      int
}

func New(window int) *RollingSum {
	return &RollingSum{
		window: make([]byte, window),
		size:   window,
	}
}

func (rs *RollingSum) Write(data []byte) (n int, err error) {
	for _, c := range data {
		rs.WriteByte(c)
	}
	return len(data), nil
}

func (rs *RollingSum) WriteByte(c byte) error {
	rs.a += -uint16(rs.window[rs.i]) + uint16(c)
	rs.b += -uint16(rs.size)*uint16(rs.window[rs.i]) + rs.a

	rs.window[rs.i] = c
	if rs.i++; rs.i == rs.size {
		rs.i = 0
	}

	return nil
}

func (rs *RollingSum) OnSplit() bool {
	return (rs.b & (blobSize - 1)) == ((^0) & (blobSize - 1))
}

func (rs *RollingSum) Reset() {
	rs.window = make([]byte, rs.size)
	rs.a, rs.b = 0, 0
}
