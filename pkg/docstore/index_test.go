package docstore

import (
	"testing"

	"a4.io/blobstash/pkg/docstore/id"
)

func TestIndexBasic(t *testing.T) {
	i, err := newSortIndex("name", "name")
	if err != nil {
		panic(err)
	}
	defer i.Close()
	defer i.db.Destroy()
	_id, _ := id.New(3)
	v := buildVal(1, _id)
	t.Logf("v=%+v\n", v)
	s, _id2 := parseVal(v)
	if s != 1 {
		t.Errorf("failed to parse start, got %q, expected 1", s)
	}
	if _id2.String() != _id.String() {
		t.Errorf("failed to parse ID, got %v, expected %v", _id2.String(), _id.String())
	}
}

func TestBuildIndexKey(t *testing.T) {
	k := buildKey("lol")
	t.Logf("k=%+v\n", k)
}

func TestIndex(t *testing.T) {
	i, err := newSortIndex("letter", "letter")
	if err != nil {
		panic(err)
	}
	defer i.Close()
	defer i.db.Destroy()

	_id1, _ := id.New(1)
	_id1.SetVersion(1)
	if err := i.Index(_id1, map[string]interface{}{"letter": "adeeee"}); err != nil {
		panic(err)
	}

	_id2, _ := id.New(2)
	_id2.SetVersion(2)
	if err := i.Index(_id2, map[string]interface{}{"letter": "aa"}); err != nil {
		panic(err)
	}

	_ids, cursor, err := i.Iter("lol", "", 50, 0)
	if err != nil {
		panic(err)
	}
	t.Logf("_ids=%q\ncursor=%v\n", _ids, cursor)

	if len(_ids) != 2 {
		t.Errorf("expected 2 _ids at first iter, got %d", len(_ids))
	}
	if _ids[0].String() != _id1.String() || _ids[0].Version() != _id1.Version() {
		t.Errorf("expected first id for fist iter to be _id1")
	}
	if _ids[1].String() != _id2.String() || _ids[1].Version() != _id2.Version() {
		t.Errorf("expected second id for fist iter to be _id2")
	}

	_id21, _ := id.FromHex(_id2.String())
	_id21.SetVersion(3)
	if err := i.Index(_id21, map[string]interface{}{"letter": "z"}); err != nil {
		panic(err)
	}

	_ids2, cursor2, err := i.Iter("lol", "", 50, 0)
	if err != nil {
		panic(err)
	}
	t.Logf("_ids2=%q\ncursor2=%v\n", _ids2, cursor2)
	if len(_ids2) != 2 {
		t.Errorf("expected 2 _ids2 at second iter, got %d", len(_ids2))
	}
	if _ids2[0].String() != _id21.String() || _ids2[0].Version() != _id21.Version() {
		t.Errorf("expected first id for second iter to be _id21")
	}
	if _ids2[1].String() != _id1.String() || _ids2[1].Version() != _id1.Version() {
		t.Errorf("expected second id for second iter to be _id1")
	}

	_ids3, cursor3, err := i.Iter("lol", "", 50, 2)
	if err != nil {
		panic(err)
	}
	t.Logf("_ids3=%q\ncursor3=%v\n", _ids3, cursor3)

	if len(_ids3) != 2 {
		t.Errorf("expected 2 _ids at third iter, got %d", len(_ids3))
	}
	if _ids3[0].String() != _id1.String() || _ids3[0].Version() != _id1.Version() {
		t.Errorf("expected first id for third iter to be _id1")
	}
	if _ids3[1].String() != _id2.String() || _ids3[1].Version() != _id2.Version() {
		t.Errorf("expected second id for third iter to be _id2")
	}
}

func TestIndexUpdatedField(t *testing.T) {
	i, err := newSortIndex("letter", "_updated")
	if err != nil {
		panic(err)
	}
	defer i.Close()
	defer i.db.Destroy()

	_id1, _ := id.New(2)
	_id1.SetVersion(2)
	if err := i.Index(_id1, map[string]interface{}{"letter": "adeeee"}); err != nil {
		panic(err)
	}

	_id2, _ := id.New(1)
	_id2.SetVersion(1)
	if err := i.Index(_id2, map[string]interface{}{"letter": "aa"}); err != nil {
		panic(err)
	}

	_ids, cursor, err := i.Iter("lol", "", 50, 0)
	if err != nil {
		panic(err)
	}
	t.Logf("_ids=%q\ncursor=%v\n", _ids, cursor)

	if len(_ids) != 2 {
		t.Errorf("expected 2 _ids at first iter, got %d", len(_ids))
	}
	if _ids[0].String() != _id1.String() || _ids[0].Version() != _id1.Version() {
		t.Errorf("expected first id for fist iter to be _id1")
	}
	if _ids[1].String() != _id2.String() || _ids[1].Version() != _id2.Version() {
		t.Errorf("expected second id for fist iter to be _id2")
	}

	_id21, _ := id.FromHex(_id2.String())
	_id21.SetVersion(3)
	if err := i.Index(_id21, map[string]interface{}{"letter": "z"}); err != nil {
		panic(err)
	}

	_ids2, cursor2, err := i.Iter("lol", "", 50, 0)
	if err != nil {
		panic(err)
	}
	t.Logf("_ids2=%q\ncursor2=%v\n", _ids2, cursor2)
	if len(_ids2) != 2 {
		t.Errorf("expected 2 _ids2 at second iter, got %d", len(_ids2))
	}
	if _ids2[0].String() != _id21.String() || _ids2[0].Version() != _id21.Version() {
		t.Errorf("expected first id for second iter to be _id21")
	}
	if _ids2[1].String() != _id1.String() || _ids2[1].Version() != _id1.Version() {
		t.Errorf("expected second id for second iter to be _id1")
	}

	_ids3, cursor3, err := i.Iter("lol", "", 50, 2)
	if err != nil {
		panic(err)
	}
	t.Logf("_ids3=%q\ncursor3=%v\n", _ids3, cursor3)

	if len(_ids3) != 2 {
		t.Errorf("expected 2 _ids at third iter, got %d", len(_ids3))
	}
	if _ids3[0].String() != _id1.String() || _ids3[0].Version() != _id1.Version() {
		t.Errorf("expected first id for third iter to be _id1")
	}
	if _ids3[1].String() != _id2.String() || _ids3[1].Version() != _id2.Version() {
		t.Errorf("expected second id for third iter to be _id2")
	}
}
