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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"runtime"
	"time"

	"github.com/bsm/redeo"

	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/pubsub"
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
	BlobRouter *backend.Router
	loadMetaBlobs sync.Once
)

var blobPutJobPool = sync.Pool{
	New: func() interface{} { return &blobPutJob{} },
}

var nCPU = runtime.NumCPU()

func init() {
	if nCPU < 2 {
		nCPU = 2
	}
	runtime.GOMAXPROCS(nCPU)
}

type ServerCtx struct {
	TxID string
	ArchiveMode bool
	Hostname string
}

type blobPutJob struct {
	req *backend.Request
	hash string
	blob []byte
	txm *backend.TxManager
}

func (j *blobPutJob) String() string {
	return fmt.Sprintf("[blobPutJob  hash=%v, meta=%v, host=%v]", j.hash, j.req.MetaBlob, j.req.Host)
}
func (j *blobPutJob) free() {
	j.req = nil
	j.blob = nil
	j.hash = ""
	j.txm = nil
	blobPutJobPool.Put(j)
}

func newBlobPutJob(req *backend.Request, hash string, blob []byte, txm *backend.TxManager) *blobPutJob {
	j := blobPutJobPool.Get().(*blobPutJob)
	j.req = req
	j.hash = hash
	j.blob = blob
	j.txm = txm
	return j
}

//	dbm.TxManagers[dbname] = NewTxManager(newdb, dbm.metaBackend)
//func SetupDB(path string) {
//	os.MkdirAll(path, 0700)
//	db, err := db.New(filepath.Join(path, "blobstash-index"))
//	if err != nil {
//		panic(err)
//	}
//	DB = db
//}

func (ctx *ServerCtx) TxManager() *backend.TxManager {
	return BlobRouter.TxManager(&backend.Request{Host: ctx.Hostname})
}

func (ctx *ServerCtx) ReqBuffer(name string) *backend.ReqBuffer {
	//return ctx.TxManager.ReqBuffer(name)
	return BlobRouter.TxManager(&backend.Request{Host: ctx.Hostname}).GetReqBuffer(name)
}

func (ctx *ServerCtx) DB() *db.DB {
	return BlobRouter.DB(&backend.Request{Host: ctx.Hostname})
}

func SetUpCtx(req *redeo.Request) {
	client := req.Client().RemoteAddr
	reqName := strings.ToLower(req.Name)
	if req.Client().Ctx == nil {
		totalConnectionsReceivedVar.Add(1)
		log.Printf("server: new connection from %+v", client)
		req.Client().Ctx = &ServerCtx{}
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
		rb := req.Client().Ctx.(*ServerCtx).ReqBuffer(txID)
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

func New(addr, webAddr, dbpath string, blobRouter *backend.Router, stop chan bool) {
	glacierPubSub := pubsub.NewPubSub("glacier")
	log.Println("server: starting...")
	BlobRouter = blobRouter
	srv := redeo.NewServer(&redeo.Config{Addr: addr})
	srv.HandleFunc("ping", func(out *redeo.Responder, _ *redeo.Request) error {
		out.WriteInlineString("PONG")
		return nil
	})
	srv.HandleFunc("now", func(out *redeo.Responder, _ *redeo.Request) error {
		out.WriteInlineString(strconv.Itoa(int(time.Now().UTC().Unix())))
		return nil
	})

	srv.HandleFunc("signal", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 2)
		if err != nil {
			return err
		}
		channel := req.Args[0]
		switch {
		case channel == "glacier":
			glacierPubSub.Publish(req.Args[1])
		default:
			log.Printf("server: invalid channel %v", channel)
			return ErrSomethingWentWrong
		}
		out.WriteOK()
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
		res, err := blobRouter.Index.Get(req.Args[0])
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
		err = blobRouter.Index.Put(req.Args[0], req.Args[1])
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
		kvs, err := blobRouter.Index.GetStringRange(req.Args[0], req.Args[1], limit)
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
		ctx := req.Client().Ctx.(*ServerCtx)
		cnt, err := ctx.DB().Sadd(req.Args[0], cmdArgs...)
		if err != nil {
			return ErrSomethingWentWrong
		}
		// Also write the data in the router index
		if strings.HasPrefix(req.Args[0], "_") {
			if _, err := blobRouter.Index.Sadd(req.Args[0], cmdArgs...); err != nil {
				return ErrSomethingWentWrong
			}
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
		ctx := req.Client().Ctx.(*ServerCtx)
		source := ctx.DB()
		if strings.HasPrefix(req.Args[0], "_") {
			// Read from the router index
			source = blobRouter.Index
		}
		cnt, err := source.Scard(req.Args[0])
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
		ctx := req.Client().Ctx.(*ServerCtx)
		source := ctx.DB()
		if strings.HasPrefix(req.Args[0], "_") {
			// Read from the router index
			source = blobRouter.Index
		}
		members := source.Smembers(req.Args[0])
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
	//srv.HandleFunc("hset", func(out *redeo.Responder, req *redeo.Request) error {
	//	SetUpCtx(req)
	//	err := CheckArgs(req, 3)
	//	if err != nil {
	//		return err
	//	}
	//	ctx := req.Client().Ctx.(*ServerCtx)
	//	cnt, err := ctx.DB().Hmset(req.Args[0], req.Args[1], req.Args[2])
	//	if err != nil {
	//		return ErrSomethingWentWrong
	//	}
	//	out.WriteInt(cnt)
	//	return nil
	//})
	srv.HandleFunc("hmset", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckMinArgs(req, 3)
		if err != nil {
			return err
		}
		cmdArgs := make([]string, len(req.Args)-1)
		copy(cmdArgs, req.Args[1:])
		ctx := req.Client().Ctx.(*ServerCtx)
		cnt, err := ctx.DB().Hmset(req.Args[0], cmdArgs...)
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
		ctx := req.Client().Ctx.(*ServerCtx)
		cnt, err := ctx.DB().Hlen(req.Args[0])
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
		ctx := req.Client().Ctx.(*ServerCtx)
		res, err := ctx.DB().Hget(req.Args[0], req.Args[1])
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
		ctx := req.Client().Ctx.(*ServerCtx)
		kvs, err := ctx.DB().Hgetall(req.Args[0])
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
		ctx := req.Client().Ctx.(*ServerCtx)
		hkeys, err := ctx.DB().Hscan(req.Args[0], req.Args[1], limit)
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
	jobc := make(chan *blobPutJob)
	for w := 1; w <= 25; w++ {
		go func(jobc <-chan *blobPutJob, w int) {
			for {
				job := <-jobc
				defer job.free()
				log.Printf("Worker %v got Job %v", w, job)
				if err := BlobRouter.Put(job.req, job.hash, job.blob); err != nil {
					panic(err)
				}
				if job.req.MetaBlob {
					if err := job.txm.LoadIncomingBlob(job.hash, job.blob); err != nil {
						panic(err)
					}
				}
			}
		}(jobc, w)
	}
	jobc2 := make(chan *blobPutJob)
	for w := 1; w <= nCPU*2; w++ {
		go func(jobc <-chan *blobPutJob, w int) {
			for {
				job := <-jobc
				defer job.free()
				log.Printf("Worker %v got Job %v", w, job)
				if err := BlobRouter.Put(job.req, job.hash, job.blob); err != nil {
					panic(err)
				}
				if job.req.MetaBlob {
					if err := job.txm.LoadIncomingBlob(job.hash, job.blob); err != nil {
						panic(err)
					}
				}
			}
		}(jobc2, w)
	}
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
		jobc<- newBlobPutJob(&backend.Request{Host: ctx.Hostname, MetaBlob: false}, sha, blob, nil)
		//err = BlobRouter.Put(&backend.Request{Host: ctx.Hostname, MetaBlob: false}, sha, blob)
		//if err != nil {
		//	log.Printf("server: Error BPUT:%v\nBlob %v:%v", err, sha, blob)
		//	return ErrSomethingWentWrong
		//}
		out.WriteString(sha)
		return nil
	})
	srv.HandleFunc("bmultiput", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		ctx := req.Client().Ctx.(*ServerCtx)
		err := CheckMinArgs(req, 1)
		if err != nil {
			return err
		}
		out.WriteBulkLen(len(req.Args))
		for _, sblob := range req.Args {
			blob := []byte(sblob)
			sha := SHA1(blob)
			out.WriteString(sha)
			jobc<- newBlobPutJob(&backend.Request{Host: ctx.Hostname, MetaBlob: false}, sha, blob, nil)
		}
		//err = BlobRouter.Put(&backend.Request{Host: ctx.Hostname, MetaBlob: false}, sha, blob)
		//if err != nil {
		//	log.Printf("server: Error BPUT:%v\nBlob %v:%v", err, sha, blob)
		//	return ErrSomethingWentWrong
		//}
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
	srv.HandleFunc("bmultiexists", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		ctx := req.Client().Ctx.(*ServerCtx)
		err := CheckMinArgs(req, 1)
		if err != nil {
			return err
		}
		out.WriteBulkLen(len(req.Args))
		for _, h := range req.Args {
			res := "0"
			exists := BlobRouter.Exists(&backend.Request{Host: ctx.Hostname, MetaBlob: false}, h)
			if exists {
				res = "1"
			}
			out.WriteString(res)
		}
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
		txm := req.Client().Ctx.(*ServerCtx).TxManager()
		jobc2<- newBlobPutJob(&backend.Request{Host: ctx.Hostname, MetaBlob: true}, sha, blob, txm)
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
		ctx := req.Client().Ctx.(*ServerCtx)
		card, err := ctx.DB().Llen(req.Args[0])
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
		ctx := req.Client().Ctx.(*ServerCtx)
		err = ctx.DB().Ladd(req.Args[0], cindex, req.Args[2])
		if err != nil {
			return ErrSomethingWentWrong
		}
		if strings.HasPrefix(req.Args[0], "_") {
			if err := blobRouter.Index.Ladd(req.Args[0], cindex, req.Args[2]); err != nil {
				return ErrSomethingWentWrong
			}
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
		ctx := req.Client().Ctx.(*ServerCtx)
		res, err := ctx.DB().Lindex(req.Args[0], cindex)
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
		ctx := req.Client().Ctx.(*ServerCtx)
		kvs, err := ctx.DB().GetListRange(req.Args[0], req.Args[1], req.Args[2], limit)
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
		ctx := req.Client().Ctx.(*ServerCtx)
		source := ctx.DB()
		if strings.HasPrefix(req.Args[0], "_") {
			source = blobRouter.Index
		}
		if len(req.Args) == 1 {
			vals, err := source.Liter(req.Args[0])
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
			ivs, err := source.LiterWithIndex(req.Args[0])
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
		// Remove this command ?
		out.WriteOK()
		return nil
	})
	srv.HandleFunc("debugctx", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		ctx := req.Client().Ctx.(*ServerCtx)
		out.WriteBulkLen(6)
		out.WriteString("tx-id")
		out.WriteString(ctx.TxID)
		out.WriteString("hostname")
		out.WriteString(ctx.Hostname)
		out.WriteString("archive-mode")
		out.WriteString(strconv.FormatBool(ctx.ArchiveMode))
		return nil
	})
	srv.HandleFunc("setctx", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckMinArgs(req, 0)
		if err != nil {
			return err
		}
		switch {
		case len(req.Args) == 2:
			ctx := req.Client().Ctx.(*ServerCtx)
			ctx.Hostname = req.Args[0]
			archiveMode, err := strconv.ParseBool(req.Args[1])
			if err != nil {
				ctx.ArchiveMode = archiveMode
			}
			out.WriteOK()
		default:
			return ErrSomethingWentWrong
		}
		return nil
	})
	srv.HandleFunc("txinit", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckMinArgs(req, 0)
		if err != nil {
			return err
		}
		switch {
		case len(req.Args) == 2:
			newID := NewID()
			ctx := req.Client().Ctx.(*ServerCtx)
			ctx.TxID = newID
			ctx.Hostname = req.Args[0]
			archiveMode, err := strconv.ParseBool(req.Args[1])
			if err != nil {
				ctx.ArchiveMode = archiveMode
			}
			out.WriteString(newID)

		case len(req.Args) == 1:
			req.Client().Ctx.(*ServerCtx).TxID = req.Args[0]
			out.WriteOK()

		default:
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
		req.Client().Ctx.(*ServerCtx).ReqBuffer(txID).Reset()
		out.WriteOK()
		return nil
	})
	rbc := make(chan *backend.ReqBuffer)
	for w := 1; w <= nCPU; w++ {
		go func(rbc <-chan *backend.ReqBuffer, w int) {
			for {
				rb := <-rbc
				log.Printf("Worker %v got ReqBuffer", w)
				if err := rb.Save(); err != nil {
					panic(err)
				}
			}
		}(rbc, w)
	}
	srv.HandleFunc("txcommit", func(out *redeo.Responder, req *redeo.Request) error {
		SetUpCtx(req)
		err := CheckArgs(req, 0)
		if err != nil {
			return err
		}
		txID := req.Client().Ctx.(*ServerCtx).TxID
		rbc <-req.Client().Ctx.(*ServerCtx).ReqBuffer(txID)
		//err = rb.Save()
		//if err != nil {
		//	log.Printf("server: TXCOMMIT error %v/%v", err, txID)
		//	return ErrSomethingWentWrong
		//}
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
	log.Printf("server: http server listening on http://%v", webAddr)
	http.HandleFunc("/", handler)
	http.HandleFunc("/debug/monitor", monitor)
	go http.ListenAndServe(webAddr, nil)

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
