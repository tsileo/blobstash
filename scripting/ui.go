package scripting

import (
	"net/http"
	"log"
)

func ScriptDebugUI() func(http.ResponseWriter, *http.Request) {
	return func (w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET":
			tpl, err := Asset("templates/ui.html")
			if err != nil {
				log.Printf("Error: %v", err)
			}
			w.Write(tpl)
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	}
}
