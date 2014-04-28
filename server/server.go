package server

import (
	"github.com/bsm/redeo"
	"github.com/tsileo/datadatabase/db"
	"github.com/tsileo/datadatabase/backend"
	"log"
	"fmt"
	"sync"
	"crypto/sha1"
	"net"
	"errors"
	"strconv"
	"os"
)

var (
	ErrInvalidDB = errors.New("redeo: invalid DB index")
	ErrSomethingWentWrong = errors.New("redeo: something went wrong")
)

type ServerCtx struct {
	DB string
	Dbm *DBsManager
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
		newdb, err := db.New(fmt.Sprintf("./ldb_%v", dbname))
		if err != nil {
			panic(err)
		}
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
		log.Println("New con")
		req.Client().Ctx = &ServerCtx{"default", dbmananger}
	}
}

func CheckArgs(req *redeo.Request, argsCnt int) error {
	//log.Printf("%+v", req)
	if len(req.Args) != argsCnt {
		return redeo.ErrWrongNumberOfArgs
	}
	return nil
}

func CheckMinArgs(req *redeo.Request, argsCnt int) error {
	log.Printf("%+v", req)
	if len(req.Args) < argsCnt {
		return redeo.ErrWrongNumberOfArgs
	}
	return nil
}

func SHA1(data []byte) string {
	h := sha1.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

var dbmananger *DBsManager

func New() {
	stop := make(chan bool)
	dbmananger = &DBsManager{DBs: make(map[string]*db.DB), mutex:&sync.Mutex{}}
	localBackend := backend.NewLocalBackend("./tmp_blobs")
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
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		err  = cdb.Put(req.Args[0], req.Args[1])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("sadd", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckMinArgs(req, 2)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		cmdArgs := make([]string, len(req.Args)-1)
		copy(cmdArgs, req.Args[1:])
		cnt := cdb.Sadd(req.Args[0], cmdArgs...)
		//if err != nil {
		//	return ErrSomethingWentWrong
		//}
		out.WriteInt(cnt)
		return nil
	})
	srv.HandleFunc("smembers", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		members := cdb.Smembers(req.Args[0])
		//if err != nil {
		//	return ErrSomethingWentWrong
		//}
		if len(members) != 0 {
			out.WriteBulkLen(len(members))
			for _, member := range members {
				out.WriteString(string(member))
			}
		} else {
			out.WriteNil()
		}
		return nil
	})
	srv.HandleFunc("hset", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 3)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		cnt, err  := cdb.Hset(req.Args[0], req.Args[1], req.Args[2])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteInt(cnt)
		return nil
	})
	srv.HandleFunc("hget", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 2)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		res, err := cdb.Hget(req.Args[0], req.Args[1])
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
	srv.HandleFunc("hgetall", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		kvs, err := cdb.Hgetall(req.Args[0])
		if err != nil {
			return ErrSomethingWentWrong
		}
		if len(kvs) != 0 {
			out.WriteBulkLen(len(kvs) * 2)
			for _, kv := range kvs {
				out.WriteString(kv.Key)
				out.WriteString(kv.Value)
			}
			//out.WriteString(string(res))
		} else {
			out.WriteNil()
		}
		return nil
	})
	srv.HandleFunc("bsize", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 0)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		size, err := cdb.GetBlobsSize()
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteInt(int(size))
		return nil
	})
	srv.HandleFunc("bcnt", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 0)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		cnt, err := cdb.GetBlobsCnt()
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteInt(int(cnt))
		return nil
	})
	srv.HandleFunc("bput", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		blob := []byte(req.Args[0])
		log.Printf("Blob len: %v", len(blob))
		sha := SHA1(blob)
		err  = localBackend.Put(sha, blob)
		if err != nil {
			return ErrSomethingWentWrong
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		cdb.IncrBlobsCnt(1)
		cdb.IncrBlobsSize(len(blob))
		out.WriteString(sha)
		return nil
	})
	srv.HandleFunc("bget", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		blob, err  := localBackend.Get(req.Args[0])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteString(string(blob))
		return nil
	})

	srv.HandleFunc("bexists", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		exists := localBackend.Exists(req.Args[0])
		res := 0
		if exists {
			res = 1
		}
		out.WriteInt(res)
		return nil
	})
	
	srv.HandleFunc("llen", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		card, err  := cdb.Llen(req.Args[0])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteInt(card)
		return nil
	})
	srv.HandleFunc("ladd", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 3)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		cindex, err := strconv.Atoi(req.Args[1])
		if err != nil {
			return ErrSomethingWentWrong
		}
		err  = cdb.Ladd(req.Args[0], cindex, req.Args[2])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("lindex", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 2)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		cindex, err := strconv.Atoi(req.Args[1])
		if err != nil {
			return ErrSomethingWentWrong
		}
		res, err := cdb.Lindex(req.Args[0], cindex)
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
	srv.HandleFunc("lrange", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 5)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		limit, err := strconv.Atoi(req.Args[4])
		if err != nil {
			return ErrSomethingWentWrong
		}
		kvs, err := cdb.GetListRange(req.Args[0], req.Args[1], req.Args[2], req.Args[3], limit)
		if err != nil {
			return ErrSomethingWentWrong
		}
		if len(kvs) == 0 {
			out.WriteNil()	
		} else {
			out.WriteBulkLen(len(kvs))
			for _, kv := range kvs {
				out.WriteString(kv.Value)
			}
		}
		return nil
	})

	srv.HandleFunc("snapshot", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 0)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		_, snapId := cdb.CreateSnapshot()
		out.WriteString(snapId)
		return nil
	})
	srv.HandleFunc("snaprelease", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		cdb := req.Client().Ctx.(*ServerCtx).GetDb()
		cdb.ReleaseSnapshot(req.Args[0])
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("shutdown", func(out *redeo.Responder, _ *redeo.Request) error {
		stop <-true
		out.WriteOK()
		return nil
	})

	log.Printf("Listening on tcp://%s", srv.Addr())
	//log.Fatal(srv.ListenAndServe())

	listener, err := net.Listen("tcp", srv.Addr())
	if err != nil {
		panic(err)
	}
	if stop != nil {
		go func() {
			for {
				if flag := <-stop; flag {
					err := listener.Close()
					if err != nil {
						os.Stderr.WriteString(err.Error())
					}
					os.Exit(1)
					return
				}
			}
		}()
	}
	errs := make(chan error)
	srv.Serve(errs, listener)
	// I know, that's a ugly and depending on undocumented behavior.
	// But when the implementation changes, we'll see it immediately as panic.
	// To the keepers of the Go standard libraries:
	// It would be useful to return a documented error type
	// when the network connection is closed.
	//if !strings.Contains(err.Error(), "use of closed network connection") {
	//	panic(err)
	//}
	log.Println("Server shutdown")

}
