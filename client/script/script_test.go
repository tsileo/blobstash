package script

import (
	"testing"

	"github.com/tsileo/blobstash/test"
	"github.com/garyburd/redigo/redis"
)

var testData = []struct {
	dest     interface{}
	args     map[string]interface{}
	code     string
	setup func(redis.Conn) error
	expected func(*testing.T, interface{})
}{
	{
		map[string]interface{}{},
		map[string]interface{}{},
		"return {Hello = 'World'}",
		func(c redis.Conn) error {
			return nil
		},
		func(t *testing.T, res interface{}) {
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
		func(t *testing.T, res interface{}) {
			if res.(map[string]interface{})["res"].(string) != "val1" {
				t.Errorf("DB script #1 failed: %+v", res)
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
		tdata.expected(t, tdata.dest)
	}
}
