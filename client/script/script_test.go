package script

import (
	"testing"
	"time"

	"github.com/tsileo/blobstash/test"
	"github.com/garyburd/redigo/redis"
)

var testData = []struct {
	dest     interface{}
	args     map[string]interface{}
	code     string
	setup func(redis.Conn) error
	expected func(*testing.T, redis.Conn, interface{})
}{
	{
		map[string]interface{}{},
		map[string]interface{}{},
		"return {Hello = 'World'}",
		func(c redis.Conn) error {
			return nil
		},
		func(t *testing.T, c redis.Conn, res interface{}) {
			if res.(map[string]interface{})["Hello"].(string) != "World" {
				t.Errorf("dummyScript failed")
			}
		},
	},
	{
		map[string]interface{}{},
		map[string]interface{}{},
		`local val = blobstash.DB.Get("k1")
		return {res = val}`,
		func(c redis.Conn) error {
			_, err := c.Do("SET", "k1", "val1")
			return err
		},
		func(t *testing.T, c redis.Conn, res interface{}) {
			if res.(map[string]interface{})["res"].(string) != "val1" {
				t.Errorf("DB script  READ#1 failed: %+v", res)
			}
		},
	},
	{
		map[string]interface{}{},
		map[string]interface{}{},
		`local val = blobstash.Tx.Set("k2", "val2")
		return {}`,
		func(c redis.Conn) error {
			return nil
		},
		func(t *testing.T, c redis.Conn, res interface{}) {
			time.Sleep(time.Second)
			val, _ := redis.String(c.Do("GET", "k2"))
			if val != "val2" {
				t.Errorf("DB script TX#1 failed: %+v, expected %v, got %v", res, "val2", val)
			}
		},
	},
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestScripting(t *testing.T) {
	s, err := test.NewTestServer(t)
	check(err)
	go s.Start()
	if err := s.TillReady(); err != nil {
		t.Fatalf("server error:\n%v", err)
	}
	defer s.Shutdown()

	c, err := redis.Dial("tcp", ":9735")
	check(err)
	defer c.Close()

	for _, tdata := range testData {
		check(tdata.setup(c))
		err := RunScript("", tdata.code, tdata.args, &tdata.dest)
		check(err)
		tdata.expected(t, c, tdata.dest)
	}
}
