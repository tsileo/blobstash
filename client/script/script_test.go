package script

import (
	"testing"

	"github.com/tsileo/blobstash/test"
)

const dummyScript = `
return {Hello = 'World'}
`

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

	res, err := RunScript("", dummyScript, map[string]interface{}{})
	check(err)
	if res["Hello"].(string) != "World" {
		t.Errorf("dummyScript failed")
	}
}
