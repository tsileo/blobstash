package scripting

import (
    "fmt"
    "time"
    "net/http"
    "encoding/json"
    "github.com/stevedonovan/luar"
    "github.com/dchest/blake2b"
    "github.com/tsileo/blobstash/backend"
    "github.com/tsileo/blobstash/client/transaction"
)

const code2 = `
print(blake2b('ok'))
print(now())
v1, v2 = two()
print(v1)
print(v2)
print(blobstash.Tx.Set('title', blobstash.Args.title))
t = three()
ta = luar.slice2table(t)
print(ta)
for i = 1, #t do  -- #v is the size of v for lists.
  print(t[i])  -- Indices start at 1 !! SO CRAZY!
end
print(ta[1])
return {
    ok = blobstash.Tx.Len(),
    t = t
}
`

const code = `
return {
    ok = blobstash.Tx.Len(),
    test = blake2b('ok')
}
`

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

func TwoValue() (string, string) {
    return "ok1", "ok2"
}

func Test1() []string {
    return []string{"IT'S SO COOL"}
}

func execScript(args map[string]interface{}) map[string]interface{} {
    // TODO set args the JSON from req.Body
    // TODO add blobstash.DB => but read-only => remove delete function and separate DBReader from DB
    L := luar.Init()
    defer L.Close()
    luar.Register(L,"",luar.Map{
        "blake2b": Hash,
        "now": Now,
        "two": TwoValue,
        "three": Test1,
        "blobstash": luar.Map{
            "Args": args,
            "Tx": transaction.NewTransaction(),
        },
    })
    res := L.DoString(code)
    if res != nil {
        fmt.Println("Error:",res)
    }
    v := luar.CopyTableToMap(L,nil,-1)
    fmt.Println("returned map",v)
    return v.(map[string]interface{})
    //for k,v := range m {
    //    fmt.Println(k,v)
    //}
    // TODO process the transaction
    // And output JSON
}

func ScriptingHandler(router *backend.Router) func(http.ResponseWriter, *http.Request) {
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
            fmt.Printf("db:%v", db)
            //args := map[string]interface{}{"title": "ok"}
            out := execScript(data)
            WriteJSON(w, out)
            return
        default:
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }
    }
}
