package maputil

import (
	"errors"
	"strconv"
	"strings"
)

var ErrKeyNotFound = errors.New("Key not found")

// FlattenList takes a `[]interface{}` and flatten/explode each items a map key.
// e.g.: ["s1", "s2"] => {"0": "s1", "1": "s2"}
func FlattenList(l []interface{}, parent, delimiter string) map[string]interface{} {
	out := map[string]interface{}{}
	var key string
	for i, ival := range l {
		if len(parent) > 0 {
			key = parent + delimiter + strconv.Itoa(i)
		} else {
			key = strconv.Itoa(i)
		}
		switch val := ival.(type) {
		case nil, int, float64, string, bool:
			out[key] = val
		case []interface{}:
			tmpout := FlattenList(val, key, delimiter)
			for tmpkey, tmpval := range tmpout {
				out[tmpkey] = tmpval
			}
		case map[string]interface{}:
			tmpout := FlattenMap(val, key, delimiter)
			for tmpkey, tmpval := range tmpout {
				out[tmpkey] = tmpval
			}

		default:
		}
	}
	return out
}

// FlattenMap takes a `map[string]interface{}` and flatten/explode it
// e.g.: {"k1": {"k2": 1}} => {"k1.k2": 1}
func FlattenMap(m map[string]interface{}, parent, delimiter string) map[string]interface{} {
	out := map[string]interface{}{}
	for key, ival := range m {
		if len(parent) > 0 {
			key = parent + delimiter + key
		}
		switch val := ival.(type) {
		case nil, int, float64, string, bool:
			out[key] = val
		case []interface{}:
			tmpout := FlattenList(val, key, delimiter)
			for tmpk, tmpv := range tmpout {
				out[tmpk] = tmpv
			}
		case map[string]interface{}:
			tmpout := FlattenMap(val, key, delimiter)
			for tmpk, tmpv := range tmpout {
				out[tmpk] = tmpv
			}
		default:
		}
	}
	return out
}

func GetPath(path string, doc map[string]interface{}) (interface{}, error) {
	keys := strings.Split(path, ".")
	for i, key := range keys {
		if val, ok := doc[key]; ok {
			if i == len(keys)-1 {
				return val, nil
			}
			if mval, ok := val.(map[string]interface{}); ok {
				doc = mval
			} else {
				// The key exists, but it's not a map
				return nil, ErrKeyNotFound
			}
		} else {
			// The key doesn't exist
			return nil, ErrKeyNotFound
		}
	}
	return doc, nil
}
