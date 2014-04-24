package server

import (
	"github.com/bsm/redeo"
	"github.com/tsileo/datadatabase/db"
	"log"
	"fmt"
	"sync"
	"errors"
	"strconv"
)

var (
	ErrInvalidDB = errors.New("redeo: invalid DB index")
	ErrSomethingWentWrong = errors.New("redeo: something went wrong")
)

type ServerCtx struct {
	DB string
	Dbm *DBsManager
	TxCommands []*redeo.Request
	TxMode bool
}

type DBsManager struct {
	DBs map[string]*db.DB
	mutex *sync.Mutex
}

func (dbm *DBsManager) GetDb(dbname string) *db.DB { 
	dbm.mutex.Lock()
	defer dbm.mutex.Unlock()
	cdb, exists := dbm.DBs[dbname]
	if !exists {
		newdb := db.New(fmt.Sprintf("./ldb_%v", dbname))
		go newdb.SnapshotHandler()
		dbm.DBs[dbname] = newdb
		return newdb
	}
	return cdb
}

func (ctx *ServerCtx) GetDb() *db.DB {
	return ctx.Dbm.GetDb(ctx.DB)
}

func SetUpCtx(req *redeo.Request) {
	if req.Client().Ctx == nil {
		req.Client().Ctx = &ServerCtx{"default", dbmananger, []*redeo.Request{}, false}
	}
}
func CheckArgs(req *redeo.Request, argsCnt int) error {
	if len(req.Args) != argsCnt {
		return redeo.ErrWrongNumberOfArgs
	}
	return nil
}

func CheckTxMode(req *redeo.Request, cmd string) bool {
	ctx := req.Client().Ctx.(*ServerCtx)
	if ctx.TxMode {
		ctx.TxCommands = append(ctx.TxCommands, req)

		return true
	}
	return false
}

var dbmananger *DBsManager

func New() {
	dbmananger = &DBsManager{DBs: make(map[string]*db.DB), mutex:&sync.Mutex{}}
	srv := redeo.NewServer(nil)
	srv.HandleFunc("ping", func(out *redeo.Responder, _ *redeo.Request) error {
		out.WriteInlineString("PONG")
		return nil
	})
	srv.HandleFunc("select", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		txmode := CheckTxMode(req, "select")
		if txmode {
			out.WriteInlineString("QUEUED")
			return nil
		}
		req.Client().Ctx.(*ServerCtx).DB = req.Args[0]
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("get", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		txmode := CheckTxMode(req, "get")
		if txmode {
			out.WriteInlineString("QUEUED")
			return nil
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		res, err := cdb.Get(req.Args[0])
		if err != nil {
			return ErrSomethingWentWrong
		}
		if res != nil {
			out.WriteString(string(res))	
		} else {
			out.WriteNil()
		}
		return nil
	})
	srv.HandleFunc("getset", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 2)
		if err != nil {
			return err
		}
		txmode := CheckTxMode(req, "getset")
		if txmode {
			out.WriteInlineString("QUEUED")
			return nil
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		res, err := cdb.Getset(req.Args[0], req.Args[1])
		if err != nil {
			return ErrSomethingWentWrong
		}
		if res != nil {
			out.WriteString(string(res))	
		} else {
			out.WriteNil()
		}
		return nil
	})
	srv.HandleFunc("set", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 2)
		if err != nil {
			return err
		}
		txmode := CheckTxMode(req, "set")
		if txmode {
			out.WriteInlineString("QUEUED")
			return nil
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		err  = cdb.Put(req.Args[0], req.Args[1])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteOK()
		return nil
	})


	srv.HandleFunc("bpcard", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		txmode := CheckTxMode(req, "bpcard")
		if txmode {
			out.WriteInlineString("QUEUED")
			return nil
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		card, err  := cdb.Bpcard(req.Args[0])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteInt(card)
		return nil
	})
	srv.HandleFunc("bpadd", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 3)
		if err != nil {
			return err
		}
		txmode := CheckTxMode(req, "bpadd")
		if txmode {
			out.WriteInlineString("QUEUED")
			return nil
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		cindex, err := strconv.Atoi(req.Args[1])
		if err != nil {
			return ErrSomethingWentWrong
		}
		err  = cdb.Bpadd(req.Args[0], cindex, req.Args[2])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("bpget", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 2)
		if err != nil {
			return err
		}
		txmode := CheckTxMode(req, "bpget")
		if txmode {
			out.WriteInlineString("QUEUED")
			return nil
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		cindex, err := strconv.Atoi(req.Args[1])
		if err != nil {
			return ErrSomethingWentWrong
		}
		res, err := cdb.Bpget(req.Args[0], cindex)
		if err != nil {
			return ErrSomethingWentWrong
		}
		if res != nil {
			out.WriteString(string(res))	
		} else {
			out.WriteNil()
		}
		return nil
	})
	srv.HandleFunc("snapshot", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 0)
		if err != nil {
			return err
		}
		txmode := CheckTxMode(req, "snapshot")
		if txmode {
			out.WriteInlineString("QUEUED")
			return nil
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		snapId := cdb.CreateSnapshot()
		out.WriteString(snapId)
		return nil
	})
	srv.HandleFunc("snaprelease", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		txmode := CheckTxMode(req, "snaprelease")
		if txmode {
			out.WriteInlineString("QUEUED")
			return nil
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		cdb.ReleaseSnapshot(req.Args[0])
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("multi", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		req.Client().Ctx.(*ServerCtx).TxMode = true
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("exec", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		ctx := req.Client().Ctx.(*ServerCtx)
		ctx.TxMode = false
		out.WriteBulkLen(len(ctx.TxCommands))
		for _, cmd := range ctx.TxCommands {
			res, _ := srv.Apply(cmd)
			res.WriteTo(out)
		}
		ctx.TxCommands = []*redeo.Request{}
		return nil
	})



	log.Printf("Listening on tcp://%s", srv.Addr())
	log.Fatal(srv.ListenAndServe())
}
