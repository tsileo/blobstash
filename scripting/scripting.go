package scripting

import (
    "fmt"
    "time"
    "net/http"
    "encoding/json"
    "github.com/stevedonovan/luar"
    "github.com/dchest/blake2b"
    "github.com/tsileo/blobstash/db"
    "github.com/tsileo/blobstash/backend"
    "github.com/tsileo/blobstash/client/transaction"
)

func WriteJSON(w http.ResponseWriter, data interface{}) {
    js, err := json.Marshal(data)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.Write(js)
}

func Hash(data string) string {
    return fmt.Sprintf("%x", blake2b.Sum256([]byte(data)))
}

func Now() int64 {
    return time.Now().UTC().Unix()
}

type DB struct {
    db *db.DB
}

func (db *DB) Get(key string) (string, error) {
    val, err := db.db.Get(key)
    return string(val), err
}

func execScript(db *db.DB, code string, args interface{}) map[string]interface{} {
    // TODO set args the JSON from req.Body
    // TODO add blobstash.DB => but read-only => remove delete function and separate DBReader from DB
    L := luar.Init()
    defer L.Close()
    luar.Register(L,"",luar.Map{
        "blake2b": Hash,
        "now": Now,
        "blobstash": luar.Map{
            "DB": &DB{db},
            "Args": args,
            "Tx": transaction.NewTransaction(),
        },
    })
    res := L.DoString(code)
    if res != nil {
        fmt.Println("Error:",res)
    }
    v := luar.CopyTableToMap(L,nil,-1)
    return v.(map[string]interface{})
    // TODO process the transaction
    // And output JSON
}

func ScriptingHandler(router *backend.Router) func(http.ResponseWriter, *http.Request) {
    val, err := router.DB(&backend.Request{}).Get("ok")
    fmt.Printf("val:%v, err:%v\n", val, err)
    return func (w http.ResponseWriter, r *http.Request) {
       switch {
        case r.Method == "POST":
            decoder := json.NewDecoder(r.Body)
            data := map[string]interface{}{}
            if err := decoder.Decode(&data); err != nil {
                http.Error(w, err.Error(), http.StatusInternalServerError)
            }
            req := &backend.Request{
                Namespace: r.Header.Get("BlobStash-Namespace"),
            }
            db := router.DB(req)
            fmt.Printf("Received script: %v\n", data)
            code := data["_script"].(string)
            args := data["_args"]
            out := execScript(db, code, args)
            fmt.Printf("Script out: %+v\n", out)
            WriteJSON(w, &out)
            return
        default:
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }
    }
}
