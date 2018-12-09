package sqlserver // import "a4.io/blobstash/pkg/sqlserver"
import (
	"io"

	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/auth"
	"gopkg.in/src-d/go-mysql-server.v0/server"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var engine *sqle.Engine
var db *myDB

func Setup() {
	engine = sqle.NewDefault()
	engine.AddDatabase(setupMyDB())
	engine.AddDatabase(sql.NewInformationSchemaDatabase(engine.Catalog))

	config := server.Config{
		Protocol: "tcp",
		Address:  "localhost:8051",
		Auth:     &auth.None{},
		// Auth: auth.NewNativeSingle("user", "pass", auth.AllPermissions),
	}

	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		panic(err)
	}

	s.Start()
}

type myDB struct {
	name   string
	tables map[string]sql.Table
}

func setupMyDB() *myDB {
	db = &myDB{
		name:   "blobstash",
		tables: map[string]sql.Table{},
		//	"my_table": &myTable{
		//		name: "my_table",
		//		schema: sql.Schema{
		//			{Name: "name", Type: sql.Text, Nullable: false, Source: "my_table"},
		//			{Name: "surname", Type: sql.Text, Nullable: false, Source: "my_table"},
		//		},
		//	},
	}
	return db
}

func AddTable(t sql.Table) {
	db.tables[t.Name()] = t
}

func (mdb *myDB) Name() string {
	return mdb.name
}

func (mdb *myDB) Tables() map[string]sql.Table {
	return mdb.tables
}

func NewTable(name string, schema sql.Schema, rows map[string]sql.Row) *myTable {
	return &myTable{
		name:   name,
		schema: schema,
		rows:   rows,
	}
}

type myTable struct {
	name   string
	schema sql.Schema
	rows   map[string]sql.Row
}

func (mt *myTable) Name() string {
	return mt.name
}

func (mt *myTable) String() string {
	return mt.name
}

func (mt *myTable) Schema() sql.Schema {
	return mt.schema
}

func (mt *myTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return newParitionIter([][]byte{[]byte("p1")}), nil
}

func (mt *myTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	rows := []sql.Row{}
	for _, row := range mt.rows {
		rows = append(rows, row)
	}
	return sql.RowsToRowIter(rows...), nil
}

func newParitionIter(keys [][]byte) *partitionIter {
	return &partitionIter{keys: keys}
}

type partition struct {
	k []byte
}

func (p *partition) Key() []byte {
	return p.k
}

type partitionIter struct {
	keys   [][]byte
	pos    int
	closed bool
}

func (p *partitionIter) Next() (sql.Partition, error) {
	if p.closed || p.pos >= len(p.keys) {
		return nil, io.EOF
	}

	key := p.keys[p.pos]
	p.pos++
	return &partition{key}, nil
}

func (p *partitionIter) Close() error {
	p.closed = true
	return nil
}
