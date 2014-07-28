package main

import (
    "fmt"
    "time"
    "github.com/stevedonovan/luar"
    "github.com/dchest/blake2b"
    "github.com/tsileo/blobstash/client/transaction"
)

const code = `
print(blake2b('ok'))
print(now())
v1, v2 = two()
print(v1)
print(v2)
print(blobstash.Tx.Set('title', blobstash.Args.title))
return {
    ok = blobstash.Tx.Len()
}
`

func Hash(data string) string {
    return fmt.Sprintf("%x", blake2b.Sum256([]byte(data)))
}

func Now() int64 {
    return time.Now().UTC().Unix()
}

func TwoValue() (string, string) {
    return "ok1", "ok2"
}



func main() {
    // TODO set args the JSON from req.Body
    // TODO add blobstash.DB => but read-only => remove delete function and separate DBReader from DB
    // A way to temporary debug lua
    L := luar.Init()
    defer L.Close()
    luar.Register(L,"",luar.Map{
        "blake2b": Hash,
        "now": Now,
        "two": TwoValue,
        "blobstash": luar.Map{
            "Args": map[string]interface{}{"title": "ok"},
            "Tx": transaction.NewTransaction(),
        },
    })
    res := L.DoString(code)
    if res != nil {
        fmt.Println("Error:",res)
    }
    v := luar.CopyTableToMap(L,nil,-1)
    fmt.Println("returned map",v)
    m := v.(map[string]interface{})
    for k,v := range m {
        fmt.Println(k,v)
    }
    // TODO process the transaction
    // And output JSON
}

