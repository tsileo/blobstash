package logger

import (
	"time"

	"github.com/yuin/gopher-lua"
	log "gopkg.in/inconshreveable/log15.v2"
)

type LoggerModule struct {
	logger log.Logger
	start  time.Time
}

func New(logger log.Logger, start time.Time) *LoggerModule {
	return &LoggerModule{
		start:  start,
		logger: logger,
	}
}

func (logger *LoggerModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"info":  logger.info,
		"debug": logger.debug,
	})
	L.Push(mod)
	return 1
}

func (logger *LoggerModule) debug(L *lua.LState) int {
	logger.logger.Debug(L.ToString(1), "t", time.Since(logger.start))
	return 0
}

func (logger *LoggerModule) info(L *lua.LState) int {
	logger.logger.Info(L.ToString(1), "t", time.Since(logger.start))
	return 0
}
