package docstore

import (
	"reflect"
	"strconv"
)

// flattenList takes a `[]interface{}` and flatten/explode each items a map key.
// e.g.: ["s1", "s2"] => {"0": "s1", "1": "s2"}
func flattenList(l []interface{}, parent, delimiter string) map[string]interface{} {
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
			tmpout := flattenList(val, key, delimiter)
			for tmpkey, tmpval := range tmpout {
				out[tmpkey] = tmpval
			}
		case map[string]interface{}:
			tmpout := flattenMap(val, key, delimiter)
			for tmpkey, tmpval := range tmpout {
				out[tmpkey] = tmpval
			}

		default:
		}
	}
	return out
}

// flattenMap takes a `map[string]interface{}` and flatten/explode it
// e.g.: {"k1": {"k2": 1}} => {"k1.k2": 1}
func flattenMap(m map[string]interface{}, parent, delimiter string) map[string]interface{} {
	out := map[string]interface{}{}
	for key, ival := range m {
		if len(parent) > 0 {
			key = parent + delimiter + key
		}
		switch val := ival.(type) {
		case nil, int, float64, string, bool:
			out[key] = val
		case []interface{}:
			tmpout := flattenList(val, key, delimiter)
			for tmpk, tmpv := range tmpout {
				out[tmpk] = tmpv
			}
		case map[string]interface{}:
			tmpout := flattenMap(val, key, delimiter)
			for tmpk, tmpv := range tmpout {
				out[tmpk] = tmpv
			}
		default:
		}
	}
	return out
}

// matchQuery takes a MongoDB-like query object and returns wether or not
// the given document match the query.
// The document will be flattened before the checks, to handle the dot-notation.
func matchQuery(query, odoc map[string]interface{}) bool {
	ok := true
	// Flatten the map to handle dot-notation handling
	doc := flattenMap(odoc, "", ".")
	for key, eval := range query {
		switch key {
		case "$or":
			res := false
			for _, iexpr := range eval.([]interface{}) {
				expr := iexpr.(map[string]interface{})
				res = res || matchQuery(expr, doc)
			}
			if !res {
				ok = false
			}
		case "$and":
			res := true
			for _, iexpr := range eval.([]interface{}) {
				expr := iexpr.(map[string]interface{})
				res = res && matchQuery(expr, doc)
			}
			ok = res
		default:
			val, check := doc[key]
			if !check {
				return false
			}
			switch eeval := eval.(type) {
			// basic `{ <field>: <value> }` query
			case nil, int, float64, string, bool, []interface{}:
				ok = ok && reflect.DeepEqual(eval, val)
			// query like `{ <field>: { <$operator>: <value> } }`
			case map[string]interface{}:
				for k, v := range eeval {
					switch k {
					case "$eq":
						ok = ok && reflect.DeepEqual(v, val)
					case "$gt":
						switch vv := v.(type) {
						case float64:
							ok = ok && val.(float64) > vv
						case int:
							ok = ok && val.(float64) > float64(vv)
						default:
							// FIXME(ts) should log a warning or a custom error
							return false
						}
					case "$gte":
						switch vv := v.(type) {
						case float64:
							ok = ok && val.(float64) >= vv
						case int:
							ok = ok && val.(float64) >= float64(vv)
						default:
							// FIXME(ts) should log a warning or a custom error
							return false
						}
					case "$lt":
						switch vv := v.(type) {
						case float64:
							ok = ok && val.(float64) < vv
						case int:
							ok = ok && val.(float64) < float64(vv)
						default:
							// FIXME(ts) should log a warning or a custom error
							return false
						}
					case "$lte":
						switch vv := v.(type) {
						case float64:
							ok = ok && val.(float64) <= vv
						case int:
							ok = ok && val.(float64) <= float64(vv)
						default:
							// FIXME(ts) should log a warning or a custom error
							return false
						}
					default:
						// Unsupported operators
						// FIXME(ts) should log a warning here or a custom error
						return false
					}
				}
			default:
				panic("shouldn't happen")
			}
		}
	}
	return ok
}
