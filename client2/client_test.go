package client2

import (
	"testing"
	"time"
	"reflect"

	"github.com/garyburd/redigo/redis"
	"github.com/tsileo/blobstash/test"
	"github.com/tsileo/blobstash/client2/ctx"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type TestStruct struct {
	Field1 string
	Field2 bool
	Field3 int
}

func TestClient(t *testing.T) {
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
	cl, err := New("")
	check(err)
	defer cl.Close()
	testCtx := &ctx.Ctx{Namespace: ""}

	con := cl.ConnWithCtx(testCtx)
	defer con.Close()

	_, err = con.Do("TXINIT", testCtx.Args())
	check(err)

	_, err = con.Do("SADD", "testset", "a", "b", "c", "d")

	_, err = con.Do("TXCOMMIT")
	check(err)

	time.Sleep(500*time.Millisecond)

	res, err := cl.Smembers(con, "testset")
	check(err)

	if !reflect.DeepEqual([]string{"a", "b", "c", "d"}, res) {
		t.Errorf("SMEMBERS failed got %q", res)
	}

	ts := &TestStruct{
		Field1: "ok",
		Field2: true,
		Field3: 10,
	}

	tx := NewTransaction()
	tx.Sadd("anotherset", "e", "f", "g")
	tx.Sadd("anotherset", "h")
	tx.Hmset("hid", FormatStruct(ts)...)
	if err := cl.Commit(testCtx, tx); err != nil {
		panic(err)
	}
	
	time.Sleep(500*time.Millisecond)

	res, err = cl.Smembers(con, "anotherset")
	check(err)
	
	if !reflect.DeepEqual([]string{"e", "f", "g", "h"}, res) {
		t.Errorf("SMEMBERS failed got %q", res)
	}

	scard, err := cl.Scard(con, "anotherset")
	check(err)
	if scard != 4 {
		t.Errorf("\"anotherset\" cardinality should be 4, got %v", scard)
	}

	ts2 := &TestStruct{}
	if err := cl.HscanStruct(con, "hid", ts2); err != nil {
		panic(err)
	}

	if !reflect.DeepEqual(ts, ts2) {
		t.Errorf("Failed to retrieve struct, expected: %+v, got: %+v", ts, ts2)
	}

	_, err = test.GetDebug()
	check(err)
	//t.Logf("Debug: %+v", debug)
}
