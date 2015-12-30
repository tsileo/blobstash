package logger

import (
	"time"

	"github.com/yuin/gopher-lua"
	log "gopkg.in/inconshreveable/log15.v2"
)

type LogRecord struct {
	Time  time.Time
	Line  string
	Level string
	T     time.Duration
}

type LoggerModule struct {
	logger  log.Logger
	start   time.Time
	records []*LogRecord
}

func New(logger log.Logger, start time.Time) *LoggerModule {
	return &LoggerModule{
		start:   start,
		logger:  logger,
		records: []*LogRecord{},
	}
}

func (logger *LoggerModule) Records() []*LogRecord {
	return logger.records
}

// TODO(tsileo) save log as `LogRecord`s

func (logger *LoggerModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"info":  logger.info,
		"debug": logger.debug,
		"error": logger.error,
	})
	L.Push(mod)
	return 1
}

func (logger *LoggerModule) error(L *lua.LState) int {
	logger.logger.Error(L.ToString(1), "t", time.Since(logger.start))
	return 0
}

func (logger *LoggerModule) debug(L *lua.LState) int {
	logger.logger.Debug(L.ToString(1), "t", time.Since(logger.start))
	return 0
}

func (logger *LoggerModule) info(L *lua.LState) int {
	logger.logger.Info(L.ToString(1), "t", time.Since(logger.start))
	return 0
}
