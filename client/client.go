package client

import (
	"fmt"
	"os"
	"time"
	"strings"
	"strconv"

	"github.com/garyburd/redigo/redis"

	"github.com/tsileo/blobstash/client/blobstore"
	"github.com/tsileo/blobstash/client/ctx"
)

var (
	DefaultServerAddr = ":9735"
)

type Client struct {
	ServerAddr string
	Pool       *redis.Pool
	Hostname   string

	BlobStore *blobstore.BlobStore
}

// New creates a new Client
func New(serverAddr string) (*Client, error) {
	if serverAddr == "" {
		serverAddr = DefaultServerAddr
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	blobstoreAddr := ""
	addrData := strings.Split(serverAddr, ":")
	if len(addrData) == 2 {
		port, err := strconv.Atoi(addrData[1])
		if err != nil {
			panic(err)
		}
		blobstoreAddr = fmt.Sprintf("http://%v:%v", addrData[0], port+1)
	}
	c := &Client{
		ServerAddr: serverAddr,
		Hostname:   hostname,
		BlobStore:  blobstore.New(blobstoreAddr), // TODO make the client use ServerAddr port+1
	}
	if err := c.setupPool(); err != nil {
		return nil, err
	}
	return c, err
}

func (client *Client) setupPool() error {
	client.Pool = &redis.Pool{
		MaxIdle:     50,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", client.ServerAddr)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return nil
}

// Conn retrieves a connection from the connection Pool
func (client *Client) Conn() redis.Conn {
	return client.Pool.Get()
}

// ConnWithCtx retrieves a connection from the connection Pool,
// and set the provided Ctx
func (client *Client) ConnWithCtx(cctx *ctx.Ctx) redis.Conn {
	con := client.Pool.Get()
	_, err := con.Do("SETCTX", cctx.Args()...)
	if err != nil {
		panic(fmt.Errorf("failed to SETCTX: %v", err))
	}
	return con
}

// Close releases the resources used by the client
func (client *Client) Close() error {
	return nil
}
