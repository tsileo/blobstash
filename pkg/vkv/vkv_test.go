package vkv

import (
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
	if !reflect.DeepEqual(kv, kv2) {
		t.Errorf("kv not equal: %+v != %+v (%s/%s)", kv, kv2, kv.Data, kv2.Data)
	}
}

func TestDBBasic(t *testing.T) {
	db, err := New("db_base")
	defer db.Destroy()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}

	kv := &KeyValue{
		Key:     "k1",
		Data:    []byte("hello"),
		Version: -1,
	}
	kv.SetHexHash("deadbeef")
	check(db.Put(kv))

	kv = &KeyValue{
		Key:     "k1",
		Data:    []byte("nope"),
		Version: 5,
	}
	kv.SetHexHash("deadbeef")
	check(db.Put(kv))

	kv2 := &KeyValue{
		Key:  "k2",
		Data: []byte("nope"),
	}
	check(db.Put(kv2))

	gkv, err := db.Get(kv.Key, -1)
	check(err)
	t.Logf("kv=%+v", gkv)

	keys, _, err := db.Keys("k", "k\xff", -1)
	check(err)
	t.Logf("keys=%+v", keys)

	versions, _, err := db.Versions("k1", 0, -1, -1)
	check(err)
	t.Logf("versions=%q", versions)
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
			kv := &KeyValue{
				Key:     k,
				Data:    []byte(fmt.Sprintf("hello-%d-%d", i, v)),
				Version: v + 1,
			}
			check(db.Put(kv))
			eversions[k][v] = kv
		}
	}
	t.Logf("ekeys=%+v", ekeys)

	for _, k := range ekeys {
		allversions, _, err := db.Versions(k, 0, -1, -1)
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
			versions, nstart, err := db.Versions(k, 0, start, 10)
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

func TestDBIter(t *testing.T) {
	db, err := New("db_base")
	defer db.Destroy()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}
	var v int
	ekeys := []string{}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("ok%d", i)
		ekeys = append(ekeys, k)
		kv := &KeyValue{
			Key:     k,
			Data:    []byte("hello"),
			Version: v + 1,
		}

		check(db.Put(kv))
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

	keys, _, err = db.ReverseKeys("", "\xff", -1)
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
		keys, nstart, err := db.ReverseKeys("", start, 2)
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
