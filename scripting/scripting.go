package scripting

import (
    "fmt"
    "time"
    "github.com/stevedonovan/luar"
    "github.com/dchest/blake2b"
    "github.com/tsileo/blobstash/db"
    "github.com/tsileo/blobstash/client/transaction"
)
// Hash generate the 256 bits Blake2B hash, accessible within the LUA script under "blake2b('ok')".
func Hash(data string) string {
    return fmt.Sprintf("%x", blake2b.Sum256([]byte(data)))
}

// Now generates current UTC timestamp, accessible in the LUA script as "now()".
func Now() int64 {
    return time.Now().UTC().Unix()
}

// DB is a "sandboxed" read-only wrapper, accessible within the LUA script under blobstash.DB
type DB struct {
    db *db.DB
}

// Get a string key
func (db *DB) Get(key string) (string, error) {
    val, err := db.db.Get(key)
    return string(val), err
}

// ExecScript execute the LUA script "code" against the database "db" with "args" as argument.
// The script must return a table (associative array) that will be returned.
func ExecScript(db *db.DB, code string, args interface{}) (map[string]interface{}, *transaction.Transaction) {
    L := luar.Init()
    defer L.Close()
    tx := transaction.NewTransaction()
    luar.Register(L,"",luar.Map{
        "blake2b": Hash,
        "now": Now,
        "blobstash": luar.Map{
            "DB": &DB{db},
            "Args": args,
            "Tx": tx,
        },
    })
    res := L.DoString(code)
    if res != nil {
        fmt.Println("Error:",res)
    }
    v := luar.CopyTableToMap(L,nil,-1)
    return v.(map[string]interface{}), tx
    // TODO process the transaction
    // And output JSON
}
