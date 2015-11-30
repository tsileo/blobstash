package docstore

import (
	"fmt"
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
				// FIXME(tsileo) dot notation handling
				res[uk] = uv
			}
			// case "$inc":
			// ...
		default:
			return nil, fmt.Errorf("Unknown %v update operator", k)
		}
	}
	return res, nil
}
