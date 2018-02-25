package writer

import (
	"os"
	"syscall"

	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
)

func setMtime(m *rnode.RawNode, fstat os.FileInfo) {
	if stat, ok := fstat.Sys().(*syscall.Stat_t); ok {
		m.ChangeTime = int64(stat.Ctim.Sec)
	}
}
