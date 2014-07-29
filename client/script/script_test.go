package script

import (
	"testing"

	"github.com/tsileo/blobstash/test"
	"github.com/garyburd/redigo/redis"
)

var testData = []struct {
	args     map[string]interface{}
	code     string
	setup func(redis.Conn) error
	expected func(*testing.T, map[string]interface{})
}{
	{
		map[string]interface{}{},
		"return {Hello = 'World'}",
		func(c redis.Conn) error {
			return nil
		},
		func(t *testing.T, res map[string]interface{}) {
			if res["Hello"].(string) != "World" {
				t.Errorf("dummyScript failed")
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
	if _, err := c.Do("PING"); err != nil {
		t.Errorf("PING failed")
	}

	for _, tdata := range testData {
		check(tdata.setup(c))
		res, err := RunScript("", tdata.code, tdata.args)
		check(err)
		tdata.expected(t, res)
	}
}
