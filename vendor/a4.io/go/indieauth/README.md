# IndieAuth

[![Build Status](https://d.a4.io/api/badges/tsileo/indieauth/status.svg)](https://d.a4.io/tsileo/indieauth)
&nbsp; &nbsp;[![Godoc Reference](https://godoc.org/a4.io/go/indieauth?status.svg)](https://godoc.org/a4.io/go/indieauth)
&nbsp; &nbsp;[![Go Report Card](https://goreportcard.com/badge/a4.io/go/indieauth)](https://goreportcard.com/report/a4.io/go/indieauth)
&nbsp; &nbsp;[![License](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/tsileo/indieauth/master/LICENSE)

This package implements an [IndieAuth (an identity layer on top of OAuth 2.0)](https://www.w3.org/TR/indieauth/) client/authentication middleware.

It implements an **IndieAuth Client** and will use your own external Authorization Endpoint.

It was designed to replace basic authentication when restricting access to private projects, it does not support multiple users.

Relies on the [sessions package from the Gorilla web toolkit](http://www.gorillatoolkit.org/pkg/sessions).

## QuickStart

```bash
$ get get a4.io/go/indieauth
```

**Note:** If you are not using gorilla/mux, you need to wrap your handlers with [`context.ClearHandler`](http://www.gorillatoolkit.org/pkg/context#ClearHandler) to prevent leaking memory.

```go
package main

import (
        "log"
        "net/http"

        "a4.io/go/indieauth"

        "github.com/gorilla/context"
        "github.com/gorilla/sessions"
)

var cookieStore = sessions.NewCookieStore([]byte("my-secret"))

func main() {
        ia, err:= indieauth.New(cookieStore, "https://my.indie.auth.domain")
        if err != nil {
                panic(err)
        }
	iaMiddleware = ia.Middleware()
        http.HandleFunc(indieauth.DefaultRedirectPath, ia.RedirectHandler)
        http.HandleFunc("/logout", func(w http.ResponseWriter, r *http.Request) {
                indie.Logout(w, r)
        })
        http.Handle("/", iaMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                w.Write([]byte("YAY!"))
        })))
        log.Fatal(http.ListenAndServe(":8011", context.ClearHandler(http.DefaultServeMux)))
}
```
