package server

import (
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bsm/redeo"

	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/db"
)

var (
	ErrInvalidDB          = errors.New("redeo: invalid DB index")
	ErrSomethingWentWrong = errors.New("redeo: something went wrong")
)

var (
	commandStatsVar             = expvar.NewMap("server-command-stats")
	serverStartedAtVar          = expvar.NewString("server-started-at")
	totalConnectionsReceivedVar = expvar.NewInt("server-total-connections-received")
	activeMonitorClient         = expvar.NewInt("server-active-monitor-client")
)

var (
	DB *db.DB
	BlobRouter *backend.Router
	loadMetaBlobs sync.Once
)

type ServerCtx struct {
	TxID string
	Hostname string
	TxManager *TxManager
}
//	dbm.TxManagers[dbname] = NewTxManager(newdb, dbm.metaBackend)

func SetupDB(path string) {
	os.MkdirAll(path, 0700)
	db, err := db.New(filepath.Join(path, "blobstash-index"))
	if err != nil {
		panic(err)
	}
	DB = db
}

func (ctx *ServerCtx) GetReqBuffer(name string) *ReqBuffer {
	return ctx.TxManager.GetReqBuffer(name)
}

func SetUpCtx(req *redeo.Request) {
	client := req.Client().RemoteAddr
	reqName := strings.ToLower(req.Name)
	if req.Client().Ctx == nil {
		totalConnectionsReceivedVar.Add(1)
		log.Printf("server: new connection from %+v", client)
		req.Client().Ctx = &ServerCtx{"", "", NewTxManager(DB, BlobRouter)}
	}

	commandStatsVar.Add(req.Name, 1)

	// Send raw cmd over SSE
	var mCmd string
	if !strings.HasPrefix(reqName, "b") &&  !strings.HasPrefix(reqName, "mb") {
		mCmd = fmt.Sprintf("%+v command  with args %+v from client: %v", req.Name, req.Args, client)
	} else {
		switch {
		case strings.ToLower(reqName) == "bput":
			mCmd = fmt.Sprintf("%+v (blob len:%v) command from client: %v", req.Name, len(req.Args[0]), client)
		case strings.ToLower(reqName) == "bexists" || strings.ToLower(reqName) == "bget":
			mCmd = fmt.Sprintf("%+v (sha:%v) command from client: %v", req.Name, req.Args[0], client)
		default:
			mCmd = fmt.Sprintf("%+v command from client: %v", req.Name, client)
		}
	}
	go SendDebugData(mCmd)

	bufferedCmd := map[string]bool{"sadd": true, "hmset": true, "hset": true, "ladd": true, "set": true}
	// TODO(tsileo) disable writing for these command, just keep them in the buffer
	_, buffered := bufferedCmd[reqName]
	if buffered {
		reqKey := req.Args[0]
		reqArgs := make([]string, len(req.Args)-1)
		copy(reqArgs, req.Args[1:])
		txID := req.Client().Ctx.(*ServerCtx).TxID
		rb := req.Client().Ctx.(*ServerCtx).GetReqBuffer(txID)
		rb.Add(reqName, reqKey, reqArgs)
	}
}

func CheckArgs(req *redeo.Request, argsCnt int) error {
	if len(req.Args) != argsCnt {
		return redeo.ErrWrongNumberOfArgs
	}
	return nil
}

func CheckMinArgs(req *redeo.Request, argsCnt int) error {
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

// NewID generate a random hash that can be used as random key
func NewID() string {
	data := make([]byte, 16)
	rand.Read(data)
	return SHA1(data)
}

func New(addr, dbpath string, blobRouter *backend.Router, stop chan bool) {
	log.Println("server: starting...")
	BlobRouter = blobRouter
	SetupDB(dbpath)
	srv := redeo.NewServer(&redeo.Config{Addr: addr})
	srv.HandleFunc("ping", func(out *redeo.Responder, _ *redeo.Request) error {
		out.WriteInlineString("PONG")
		return nil
	})
	srv.HandleFunc("now", func(out *redeo.Responder, _ *redeo.Request) error {
		out.WriteInlineString(strconv.Itoa(int(time.Now().UTC().Unix())))
		return nil
	})
	//srv.HandleFunc("select", func(out *redeo.Responder, req *redeo.Request) error {
	//	SetUpCtx(req)
	//	err := CheckArgs(req, 1)
	//	if err != nil {
	//		return err
	//	}
	//	req.Client().Ctx.(*ServerCtx).DB = req.Args[0]
	//	out.WriteOK()
	//	return nil
	//})
	srv.HandleFunc("hash", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		out.WriteString(SHA1([]byte(req.Args[0])))
		return nil
	})
	srv.HandleFunc("get", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		res, err := DB.Get(req.Args[0])
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
	// Not needed as for now
	//srv.HandleFunc("getset", func(out *redeo.Responder, req *redeo.Request) error {
	//	SetUpCtx(req)
	//	err := CheckArgs(req, 2)
	//	if err != nil {
	//		return err
	//	}
	//	cdb := req.Client().Ctx.(*ServerCtx).DB
	//	res, err := cdb.Getset(req.Args[0], req.Args[1])
	//	if err != nil {
	//		return ErrSomethingWentWrong
	//	}
	//	if res != nil {
	//		out.WriteString(string(res))
	//	} else {
	//		out.WriteNil()
	//	}
	//	return nil
	//})
	srv.HandleFunc("set", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 2)
		if err != nil {
			return err
		}
		err = DB.Put(req.Args[0], req.Args[1])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("range", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 3)
		if err != nil {
			return err
		}
		limit, err := strconv.Atoi(req.Args[2])
		if err != nil {
			return ErrSomethingWentWrong
		}
		kvs, err := DB.GetStringRange(req.Args[0], req.Args[1], limit)
		if err != nil {
			return ErrSomethingWentWrong
		}
		if len(kvs) == 0 {
			out.WriteNil()
			return nil
		}
		out.WriteBulkLen(len(kvs))
		for _, skv := range kvs {
			out.WriteString(skv.Value)
		}
		return nil
	})
	srv.HandleFunc("sadd", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckMinArgs(req, 2)
		if err != nil {
			return err
		}
		cmdArgs := make([]string, len(req.Args)-1)
		copy(cmdArgs, req.Args[1:])
		cnt, err := DB.Sadd(req.Args[0], cmdArgs...)
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteInt(cnt)
		return nil
	})
	srv.HandleFunc("scard", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		cnt, err := DB.Scard(req.Args[0])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteInt(cnt)
		return nil
	})
	srv.HandleFunc("smembers", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		members := DB.Smembers(req.Args[0])
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
		cnt, err := DB.Hmset(req.Args[0], req.Args[1], req.Args[2])
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteInt(cnt)
		return nil
	})
	srv.HandleFunc("hmset", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckMinArgs(req, 3)
		if err != nil {
			return err
		}
		cmdArgs := make([]string, len(req.Args)-1)
		copy(cmdArgs, req.Args[1:])
		cnt, err := DB.Hmset(req.Args[0], cmdArgs...)
		if err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteInt(cnt)
		return nil
	})
	srv.HandleFunc("hlen", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckMinArgs(req, 1)
		if err != nil {
			return err
		}
		cnt, err := DB.Hlen(req.Args[0])
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
		res, err := DB.Hget(req.Args[0], req.Args[1])
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
		kvs, err := DB.Hgetall(req.Args[0])
		if err != nil {
			return ErrSomethingWentWrong
		}
		if len(kvs) != 0 {
			out.WriteBulkLen(len(kvs) * 2)
			for _, kv := range kvs {
				out.WriteString(kv.Key)
				out.WriteString(kv.Value)
			}
		} else {
			out.WriteNil()
		}
		return nil
	})
	srv.HandleFunc("hscan", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 3)
		if err != nil {
			return err
		}
		limit, err := strconv.Atoi(req.Args[2])
		if err != nil {
			return ErrSomethingWentWrong
		}
		hkeys, err := DB.Hscan(req.Args[0], req.Args[1], limit)
		if err != nil {
			return ErrSomethingWentWrong
		}
		if len(hkeys) != 0 {
			out.WriteBulkLen(len(hkeys))
			for _, hkey := range hkeys {
				out.WriteString(string(hkey))
			}
		} else {
			out.WriteNil()
		}
		return nil
	})

	// Blob related commands
	srv.HandleFunc("bput", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		ctx := req.Client().Ctx.(*ServerCtx)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		blob := []byte(req.Args[0])
		sha := SHA1(blob)
		err = BlobRouter.Put(&backend.Request{Host: ctx.Hostname, MetaBlob: false}, sha, blob)
		if err != nil {
			log.Printf("server: Error BPUT:%v\nBlob %v:%v", err, sha, blob)
			return ErrSomethingWentWrong
		}
		out.WriteString(sha)
		return nil
	})
	srv.HandleFunc("bget", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		ctx := req.Client().Ctx.(*ServerCtx)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		blob, err := BlobRouter.Get(&backend.Request{Host: ctx.Hostname, MetaBlob: false}, req.Args[0])
		if err != nil {
			log.Printf("Error bget %v: %v", req.Args[0], err)
			return ErrSomethingWentWrong
		}
		out.WriteString(string(blob))
		return nil
	})
	srv.HandleFunc("bexists", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		ctx := req.Client().Ctx.(*ServerCtx)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		exists := BlobRouter.Exists(&backend.Request{Host: ctx.Hostname, MetaBlob: false}, req.Args[0])
		res := 0
		if exists {
			res = 1
		}
		out.WriteInt(res)
		return nil
	})

	// Meta blob related commands
	srv.HandleFunc("mbput", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		ctx := req.Client().Ctx.(*ServerCtx)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		blob := []byte(req.Args[0])
		sha := SHA1(blob)
		err = BlobRouter.Put(&backend.Request{Host: ctx.Hostname, MetaBlob: true}, sha, blob)
		if err != nil {
			log.Printf("server: Error MBPUT:%v\nBlob %v:%v", err, sha, blob)
			return ErrSomethingWentWrong
		}
		if err := req.Client().Ctx.(*ServerCtx).GetReqBuffer("").LoadIncomingBlob(ctx.Hostname, sha, blob); err != nil {
			log.Printf("server: Error MBPUT:%v\nBlob %v:%v", err, sha, string(blob))
			return ErrSomethingWentWrong
		}
		out.WriteString(sha)
		return nil
	})
	srv.HandleFunc("mbget", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		ctx := req.Client().Ctx.(*ServerCtx)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		blob, err := BlobRouter.Get(&backend.Request{Host: ctx.Hostname, MetaBlob: true}, req.Args[0])
		if err != nil {
			log.Printf("Error bget %v: %v", req.Args[0], err)
			return ErrSomethingWentWrong
		}
		out.WriteString(string(blob))
		return nil
	})
	srv.HandleFunc("mbexists", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		ctx := req.Client().Ctx.(*ServerCtx)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		exists := BlobRouter.Exists(&backend.Request{Host: ctx.Hostname, MetaBlob: true}, req.Args[0])
		res := 0
		if exists {
			res = 1
		}
		out.WriteInt(res)
		return nil
	})

	// List related command
	srv.HandleFunc("llen", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		card, err := DB.Llen(req.Args[0])
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
		cindex, err := strconv.Atoi(req.Args[1])
		if err != nil {
			return ErrSomethingWentWrong
		}
		err = DB.Ladd(req.Args[0], cindex, req.Args[2])
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
		cindex, err := strconv.Atoi(req.Args[1])
		if err != nil {
			return ErrSomethingWentWrong
		}
		res, err := DB.Lindex(req.Args[0], cindex)
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
		err := CheckArgs(req, 4)
		if err != nil {
			return err
		}
		limit, err := strconv.Atoi(req.Args[3])
		if err != nil {
			return ErrSomethingWentWrong
		}
		kvs, err := DB.GetListRange(req.Args[0], req.Args[1], req.Args[2], limit)
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
	srv.HandleFunc("liter", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckMinArgs(req, 1)
		if err != nil {
			return err
		}
		if len(req.Args) == 1 {
			vals, err := DB.Liter(req.Args[0])
			if err != nil {
				return ErrSomethingWentWrong
			}
			if len(vals) == 0 {
				out.WriteNil()
			} else {
				out.WriteBulkLen(len(vals))
				for _, bval := range vals {
					out.WriteString(string(bval))
				}
			}
			return nil
		}
		if len(req.Args) == 3 {
			// WITH INDEX
			if strings.ToLower(req.Args[1]) != "with" {
				return ErrSomethingWentWrong
			}
			ivs, err := DB.LiterWithIndex(req.Args[0])
			if err != nil {
				return ErrSomethingWentWrong
			}
			if len(ivs) == 0 {
				out.WriteNil()
			} else {
				out.WriteBulkLen(len(ivs) * 2)
				for _, iv := range ivs {
					out.WriteString(strconv.Itoa(iv.Index))
					out.WriteString(iv.Value)
				}
			}
			return nil
		}
		return ErrSomethingWentWrong
	})

	srv.HandleFunc("size", func(out *redeo.Responder, _ *redeo.Request) error {
		out.WriteInt(0)
		return nil
	})
	srv.HandleFunc("shutdown", func(out *redeo.Responder, _ *redeo.Request) error {
		stop <- true
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("init", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 0)
		if err != nil {
			return err
		}
		txID := req.Client().Ctx.(*ServerCtx).TxID
		hostname := req.Client().Ctx.(*ServerCtx).Hostname
		if err := req.Client().Ctx.(*ServerCtx).GetReqBuffer(txID).Load(hostname); err != nil {
			return ErrSomethingWentWrong
		}
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("txinit", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckMinArgs(req, 0)
		if err != nil {
			return err
		}
		switch {
		case len(req.Args) == 0:
			newID := NewID()
			req.Client().Ctx.(*ServerCtx).TxID = newID
			out.WriteString(newID)

		case len(req.Args) == 1:
			req.Client().Ctx.(*ServerCtx).TxID = req.Args[0]
			out.WriteOK()

		case len(req.Args) > 1:
			return ErrSomethingWentWrong
		}
		return nil
	})
	srv.HandleFunc("txdiscard", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 0)
		if err != nil {
			return err
		}
		txID := req.Client().Ctx.(*ServerCtx).TxID
		req.Client().Ctx.(*ServerCtx).GetReqBuffer(txID).Reset()
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("txcommit", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 0)
		if err != nil {
			return err
		}
		txID := req.Client().Ctx.(*ServerCtx).TxID
		hostname := req.Client().Ctx.(*ServerCtx).Hostname
		rb := req.Client().Ctx.(*ServerCtx).GetReqBuffer(txID)
		err = rb.Save(hostname)
		if err != nil {
			log.Printf("server: TXCOMMIT error %v/%v", err, txID)
			return ErrSomethingWentWrong
		}
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("hostname", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 1)
		if err != nil {
			return err
		}
		req.Client().Ctx.(*ServerCtx).Hostname = req.Args[0]
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("done", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 0)
		if err != nil {
			return err
		}
		if err := BlobRouter.Done(); err != nil {
			log.Printf("Error BlobRouter %T Done callback: %v", BlobRouter, err)
			return ErrSomethingWentWrong
		}
		out.WriteOK()
		return nil
	})
	serverStartedAtVar.Set(time.Now().UTC().Format(time.RFC822Z))
	log.Printf("server: http server listening on http://0.0.0.0:9737")
	http.HandleFunc("/", handler)
	http.HandleFunc("/debug/monitor", monitor)
	go http.ListenAndServe("0.0.0.0:9737", nil)

	log.Printf("server: listening on tcp://%s", srv.Addr())
	//log.Fatal(srv.ListenAndServe())

	listener, err := net.Listen("tcp", srv.Addr())
	if err != nil {
		panic(err)
	}
	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	if stop != nil {
		go func() {
			for {
				select {
				case _ = <-stop:
					break
				case sig := <-cs:
					log.Printf("server: Captured %v\n", sig)
					break
				}
				log.Println("server: closing DB first...")
				DB.Close()
				log.Println("server: closing backends...")
				BlobRouter.Close()
				log.Println("server: shutting down...")
				err := listener.Close()
				if err != nil {
					log.Println(err.Error())
				}
				os.Exit(0)
			}
		}()
	}
	errs := make(chan error)
	srv.Serve(errs, listener)
}
