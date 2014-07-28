package main

import (
    "fmt"
    "github.com/stevedonovan/luar"
    "github.com/tsileo/blobstash/client/transaction"
)

const code = `
print(blobstash.Tx.Set('title', blobstash.Args.title))
return {
    ok = blobstash.Tx.Len()
}
`

func main() {
    L := luar.Init()
    defer L.Close()
    luar.Register(L,"",luar.Map{
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
}

