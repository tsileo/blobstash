package vkv

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func checkKv(t *testing.T, kv, kv2 *KeyValue) {
	if kv.Flag != kv2.Flag || kv.Version != kv2.Version || kv.Hash != kv2.Hash || !bytes.Equal(kv.Data, kv2.Data) {
		t.Errorf("failed to fetch kv %+v != %+v", kv, kv2)
	}
}

func TestDBEncoding(t *testing.T) {
	db, err := New("db_base")
	defer db.Destroy()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}
	h := "e3c8102868d28b5ff85fc35dda07329970d1a01e273c37481326fe0c861c8142"
	kv, err := db.Put("ok", h, []byte("hello"), -1)
	check(err)
	t.Logf("kv=%+v", kv)
	kv2, err := db.Get("ok", -1)
	check(err)
	t.Logf("kv2=%+v", kv2)
	checkKv(t, kv, kv2)

	kv, err = db.Put("ok", "", []byte("hello2"), -1)
	check(err)
	t.Logf("kv=%+v", kv)
	kv2, err = db.Get("ok", -1)
	check(err)
	t.Logf("kv2=%+v", kv2)
	checkKv(t, kv, kv2)

	kv, err = db.Put("ok", h, nil, -1)
	check(err)
	t.Logf("kv=%+v", kv)
	kv2, err = db.Get("ok", -1)
	check(err)
	t.Logf("kv2=%+v", kv2)
	checkKv(t, kv, kv2)
}

func TestDB(t *testing.T) {
	db, err := New("db_base")
	defer db.Destroy()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}
	kv, err := db.Put("ok", "", []byte("hello"), -1)
	kv.db = nil
	check(err)
	t.Logf("kv=%+v", kv)
	kv2, err := db.Get("ok", -1)
	check(err)
	t.Logf("kv2=%+v", kv2)
	checkKv(t, kv, kv2)
	kv3, err := db.Put("ok", "", []byte("hello2"), -1)
	check(err)
	t.Logf("kv3=%+v", kv3)
	kv4, err := db.Get("ok", -1)
	check(err)
	t.Logf("kv4=%+v", kv4)
	checkKv(t, kv3, kv4)

	versions, _, err := db.Versions("ok", -1, 0, -1)
	check(err)
	if len(versions.Versions) != 2 {
		t.Errorf("failed to fetch versions: %+v", versions)
	}
	t.Logf("versions=%+v", versions)
	checkKv(t, versions.Versions[0], kv3)
	// checkKv(t, versions.Versions[1], kv)

	kv5, err := db.Put("ok2", "", []byte("hello"), -1)
	check(err)
	t.Logf("kv5=%+v", kv5)

	versions, _, err = db.Versions("ok", -1, 0, -1)
	check(err)
	if len(versions.Versions) != 2 {
		t.Errorf("failed to fetch versions: %+v", versions)
	}
	t.Logf("versions=%+v", versions)
	checkKv(t, versions.Versions[0], kv3)
	// checkKv(t, versions.Versions[1], kv)

	if _, err := db.Put("lol", "", []byte("lol"), -1); err != nil {
		panic(err)
	}

	keys, _, err := db.Keys("", "\xff", -1)
	check(err)
	t.Logf("keys=%+v\n", keys)
	if len(keys) != 3 {
		t.Errorf("failed to fetch keys (%d keys)", len(keys))
	}
	keys, _, err = db.ReverseKeys("\xff", "", -1)
	check(err)
	t.Logf("reverse keys=%+v\n", keys)
	if len(keys) != 3 {
		t.Errorf("failed to fetch keys (%d keys)", len(keys))
	}
}

func TestDBIter(t *testing.T) {
	db, err := New("db_base")
	defer db.Destroy()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}
	ekeys := []string{}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("ok%d", i)
		ekeys = append(ekeys, k)
		kv, err := db.Put(k, "", []byte("hello"), -1)
		check(err)
		t.Logf("kv%d=%+v", i, kv)
	}
	sort.Sort(sort.StringSlice(ekeys))
	t.Logf("ekeys=%+v", ekeys)
	keys, _, err := db.Keys("", "\xff", -1)
	check(err)
	t.Logf("keys=%+v\n", keys)
	if len(keys) != 10 {
		t.Errorf("failed to fetch keys (%d keys)", len(keys))
	}
	skeys := []string{}
	for _, kv := range keys {
		skeys = append(skeys, kv.Key)
	}
	sort.Sort(sort.StringSlice(ekeys))
	if !reflect.DeepEqual(ekeys, skeys) {
		t.Errorf("bad sort order")
	}

	res := []string{}
	start := ""
	for i := 0; i < 6; i++ {
		keys, nstart, err := db.Keys(start, "\xff", 2)
		check(err)
		t.Logf("sub=%+v, start=%+v", keys, start)
		for _, k := range keys {
			res = append(res, k.Key)
		}
		start = nstart
	}
	t.Logf("res=%+v", res)
	if !reflect.DeepEqual(ekeys, res) {
		t.Errorf("key iter error")
	}

	keys, _, err = db.ReverseKeys("\xff", "", -1)
	check(err)
	t.Logf("reverse keys=%+v\n", keys)
	if len(keys) != 10 {
		t.Errorf("failed to fetch keys (%d keys)", len(keys))
	}
	skeys = []string{}
	for _, kv := range keys {
		skeys = append(skeys, kv.Key)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(ekeys)))
	if !reflect.DeepEqual(ekeys, skeys) {
		t.Errorf("bad reverse sort order")
	}

	res = []string{}
	start = "\xff"
	for i := 0; i < 6; i++ {
		keys, nstart, err := db.ReverseKeys(start, "", 2)
		check(err)
		t.Logf("sub=%+v, start=%+v, nstart=%+v", keys, start, nstart)
		for _, k := range keys {
			res = append(res, k.Key)
		}
		start = nstart
	}
	t.Logf("res=%+v", res)
	if !reflect.DeepEqual(ekeys, res) {
		t.Errorf("bad reverse sort order")
	}
}

func TestDBIVersions(t *testing.T) {
	db, err := New("db_base")
	defer db.Destroy()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}
	eversions := map[string]map[int]*KeyValue{}
	ekeys := []string{}
	vcount := 100
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("ok%d", i)
		eversions[k] = map[int]*KeyValue{}
		ekeys = append(ekeys, k)
		for v := 0; v < vcount; v++ {
			kv, err := db.Put(k, "", []byte(fmt.Sprintf("hello-%d-%d", i, v)), -1)
			check(err)
			// t.Logf("kv%d(v%d)=%+v", v, i, kv)
			eversions[k][v] = kv
		}
	}
	t.Logf("ekeys=%+v", ekeys)

	for _, k := range ekeys {
		allversions, _, err := db.Versions(k, -1, 0, -1)
		check(err)
		if len(allversions.Versions) != vcount {
			t.Errorf("failed to fetch versions: %+v", allversions)
		}
		t.Logf("versions=%+v", allversions)
		for v := 0; v < vcount; v++ {
			checkKv(t, eversions[k][int(math.Abs(float64(v-(vcount-1))))], allversions.Versions[v])
		}
		start := -1
		// j := 0
		eversions2 := []*KeyValue{}
		for i := 0; i < vcount/10; i++ {
			versions, nstart, err := db.Versions(k, start, 0, 10)
			check(err)
			if len(versions.Versions) != vcount/10 {
				t.Errorf("failed to fetch versions: %+v", versions)
			}
			for _, v := range versions.Versions {
				eversions2 = append(eversions2, v)
			}
			start = nstart
		}
		t.Logf("count=%d\n", len(eversions2))
		if len(eversions2) != vcount {
			t.Errorf("iteration failed")
		}
		for v, kv := range eversions2 {
			checkKv(t, eversions[k][int(math.Abs(float64(v-(vcount-1))))], kv)
		}
	}
}
