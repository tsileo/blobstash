package client2

import (
	"github.com/garyburd/redigo/redis"
)

func (client *Client) Smembers(con redis.Conn, key string) ([]string, error) {
	return redis.Strings(con.Do("SMEMBERS", key))
}
