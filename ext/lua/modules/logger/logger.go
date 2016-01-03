package logger

import (
	"time"

	"github.com/yuin/gopher-lua"
	log "gopkg.in/inconshreveable/log15.v2"
)

type Lvl int

const (
	LvlCrit = iota // May be used for logging panic?
	LvlError
	LvlWarn
	LvlInfo
	LvlDebug
)

// Returns the name of a Lvl
func (l Lvl) String() string {
	switch l {
	case LvlDebug:
		return "dbug"
	case LvlInfo:
		return "info"
	case LvlWarn:
		return "warn"
	case LvlError:
		return "eror"
	case LvlCrit:
		return "crit"
	default:
		panic("bad level")
	}
}

type LogRecord struct {
	Time    time.Time     `json:"time"`
	Message string        `json:"message"`
	Lvl     Lvl           `json:"lvl"`
	T       time.Duration `json:"t"`
	ReqID   string        `json:"req_id"`
}

type LoggerModule struct {
	logger  log.Logger
	start   time.Time
	reqID   string
	records []*LogRecord
}

func New(logger log.Logger, start time.Time, reqID string) *LoggerModule {
	return &LoggerModule{
		start:   start,
		logger:  logger,
		reqID:   reqID,
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
	message := L.ToString(1)
	t := time.Since(logger.start)
	logRecord := &LogRecord{
		Lvl:     LvlInfo,
		Time:    time.Now().UTC(),
		Message: message,
		T:       t,
		ReqID:   logger.reqID,
	}
	logger.records = append(logger.records, logRecord)
	logger.logger.Info(L.ToString(1), "t", time.Since(logger.start))
	return 0
}
