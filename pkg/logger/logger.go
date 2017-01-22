package logger // import "a4.io/blobstash/pkg/logger"

import (
	"os"

	log "gopkg.in/inconshreveable/log15.v2"
)

var Log = log.New()

func init() {
	Log.SetHandler(log.DiscardHandler())
}

func InitLogger(slvl string) {
	if slvl == "" {
		slvl = "debug"
	}
	lvl, err := log.LvlFromString(slvl)
	if err != nil {
		panic(err)
	}
	Log.SetHandler(log.LvlFilterHandler(lvl, log.StreamHandler(os.Stdout, log.TerminalFormat())))
	return
}
