package client2

import (
	"fmt"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"

	_ "github.com/tsileo/blobstash/disklru"
	_"github.com/tsileo/blobstash/lru"
)

var (
	DefaultServerAddr = ":9735"
)

type Client struct {
	ServerAddr   string
	Pool         *redis.Pool
	Hostname     string
}

// Ctx holds a request context
type Ctx struct {
	Namespace string
	MetaBlob bool // Flag for blob upload
}

// Args cast the Ctx into Redis.Args
func (ctx *Ctx) Args() redis.Args {
	return redis.Args{}.Add(ctx.Namespace)
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
	c := &Client{
		ServerAddr: serverAddr,
		Hostname: hostname,
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
func (client *Client) ConnWithCtx(ctx *Ctx) redis.Conn {
	con := client.Pool.Get()
	_, err := con.Do("SETCTX", ctx.Args()...)
	if err != nil {
		panic(fmt.Errorf("failed to SETCTX: %v", err))
	}
	return con
}

// Close releases the resources used by the client
func (client *Client) Close() error {
	return nil
}
