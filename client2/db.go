package client2

import (
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
)

// Smembers returns every members of the given set.
func (client *Client) Smembers(con redis.Conn, key string) ([]string, error) {
	return redis.Strings(con.Do("SMEMBERS", key))
}

// Scard returns the cardinality of the given set.
func (client *Client) Scard(con redis.Conn, key string) (int, error) {
	return redis.Int(con.Do("SCARD", key))
}

// Get fetch the value for the given key.
func (client *Client) Get(con redis.Conn, key string) (string, error) {
	return redis.String(con.Do("GET", key))
}

// Hlen returns the number of fields for the given hash key.
func (client *Client) Hlen(con redis.Conn, key string) (int, error) {
	return redis.Int(con.Do("HLEN", key))
}

// HscanStruct scans alternating names and values of the hash to given struct.
func (client *Client) HscanStruct(con redis.Conn, key string, s interface{}) (error) {
	reply, err := redis.Values(con.Do("HGETALL", key))
	if err != nil {
		return err
	}
	return redis.ScanStruct(reply, s)
}

// LLen returns the length of the given list
func (client *Client) Llen(con redis.Conn, key string) (int, error) {
	return redis.Int(con.Do("LLEN", key))
}

// Llast performs a LLAST key command, it returns the value of the last item of the list
// (with the greater index).
func (client *Client) Llast(con redis.Conn, key string) (string, error) {
	return redis.String(con.Do("LLAST", key))
}

// LlastWithIndex performs a LLAST key WITH INDEX command,
// it returns the (value, index) pair of the last item of the list (with the greater index).
func (client *Client) LlastWithIndex(con redis.Conn, key string) (string, int, error) {
	data, err := redis.Strings(con.Do("LLAST", key, "WITH", "INDEX"))
	if len(data) != 2 || err != nil {
		return "", 0, fmt.Errorf("unexpected response from server: %v/%v", data, err)
	}
	index, err := strconv.Atoi(data[0])
	if err != nil {
		return "", 0, fmt.Errorf("failed to decode index: %v", err)
	}
	return data[1], index, nil
}

// LriterScanSlice performs a LRITER key WITH INDEX command (reverse list iteration with index)
// Expects a slice of struct containing an int as first field (the index),
// and a string as second field, the value.
func (client *Client) LriterScanSlice(con redis.Conn, key string, slice interface{}) error {
	values, _ := redis.Values(con.Do("LRITER", key, "WITH", "INDEX"))
	return redis.ScanSlice(values, slice)
}
