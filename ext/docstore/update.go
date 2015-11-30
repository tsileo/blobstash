package docstore

import (
	"fmt"
	"strings"
)

func updateDoc(doc, update map[string]interface{}) (map[string]interface{}, error) {
	res := map[string]interface{}{}
	// First, we need to copy the original doc
	for k, v := range doc {
		res[k] = v
	}
	for k, v := range update {
		switch k {
		case "$set":
			toset, ok := v.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("\"$set\" argument must be a map")
			}
			for uk, uv := range toset {
				rpointer := res
				// FIXME(tsileo) dot notation handling
				if strings.Contains(uk, ".") {
					path := strings.Split(uk, ".")
					for i, key := range path {
						if i == len(path)-1 {
							// If it's the last key of the path, then update the value
							rpointer[key] = uv
						} else {
							// FIXME(ts) create object if it does not exists
							rpointer = rpointer[key].(map[string]interface{})
						}
					}

				} else {
					res[uk] = uv
				}
			}
			// case "$inc":
			// ...
		default:
			return nil, fmt.Errorf("Unknown %v update operator", k)
		}
	}
	return res, nil
}
