package client2

import (
	"github.com/garyburd/redigo/redis"
)

func (client *Client) Smembers(con redis.Conn, key string) ([]string, error) {
	return redis.Strings(con.Do("SMEMBERS", key))
}

func (client *Client) Scard(con redis.Conn, key string) (int, error) {
	return redis.Int(con.Do("SCARD", key))
}

func (client *Client) Get(con redis.Conn, key string) (string, error) {
	return redis.String(con.Do("GET", key))
}

func (client *Client) Hlen(con redis.Conn, key string) (int, error) {
	return redis.Int(con.Do("HLEN", key))
}

func (client *Client) HscanStruct(con redis.Conn, key string, s interface{}) (error) {
	reply, err := redis.Values(con.Do("HGETALL", key))
	if err != nil {
		return err
	}
	return redis.ScanStruct(reply, s)
}

func (client *Client) Llen(con redis.Conn, key string) (int, error) {
	return redis.Int(con.Do("LLEN", key))
}

