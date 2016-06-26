package nsdb

import "testing"

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func TestDB(t *testing.T) {
	// db, err := New("db_base")
	// defer db.Destroy()
	// if err != nil {
	// 	t.Fatalf("Error creating db %v", err)
	// }
	// h := "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5"
	// ns, err := db.Namespaces(h)
	// check(err)
	// if len(ns) != 0 {
	// 	t.Fatalf("Namespaces should be empty", ns)
	// }
	// check(db.AddNs(h, "ns1"))
	// check(db.AddNs(h, "ns2"))
	// ns, err = db.Namespaces(h)
	// check(err)
	// if len(ns) != 2 || ns[0] != "ns1" || ns[1] != "ns2" {
	// 	t.Errorf("bad namespaces results, expected [ns1, ns2], got %q", ns)
	// }
	// check(db.RemoveNs(h, "ns1"))
	// ns, err = db.Namespaces(h)
	// check(err)
	// if len(ns) != 1 || ns[0] != "ns2" {
	// 	t.Errorf("bad namespaces results, expected [ns2], got %q", ns)
	// }
}
