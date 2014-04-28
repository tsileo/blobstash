package models

import (
	"time"
	"github.com/garyburd/redigo/redis"
)

func GetDbPool() (pool *redis.Pool, err error) {
	pool = &redis.Pool{
		MaxIdle:     50,
		MaxActive: 50,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "localhost:9736")
			if err != nil {
				return nil, err
			}
			//if _, err := c.Do("AUTH", password); err != nil {
			//    c.Close()
			//    return nil, err
			//}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return
}

type Client struct {
	Pool *redis.Pool
}

func NewClient() (*Client, error) {
	pool, err := GetDbPool()
	return &Client{Pool:pool}, err
}
