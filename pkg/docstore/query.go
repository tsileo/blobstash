package docstore

import (
	"errors"
	"reflect"

	"a4.io/blobstash/pkg/docstore/maputil"

	log "github.com/inconshreveable/log15"
)

var ErrUnsupportedOperator = errors.New("Unsupported query operator")

// matchQuery takes a MongoDB-like query object and returns wether or not
// the given document match the query.
// The document will be flattened before the checks, to handle the dot-notation.
func matchQuery(qLogger log.Logger, query, odoc map[string]interface{}) bool {
	logger := qLogger.New("subquery", query, "doc", odoc)
	ok := true
	// Flatten the map to handle dot-notation handling
	doc := maputil.FlattenMap(odoc, "", ".")
	for key, eval := range query {
		switch key {
		case "$or":
			res := false
			for _, iexpr := range eval.([]interface{}) {
				expr := iexpr.(map[string]interface{})
				res = res || matchQuery(qLogger, expr, doc)
			}
			if !res {
				ok = false
			}
		case "$and":
			res := true
			for _, iexpr := range eval.([]interface{}) {
				expr := iexpr.(map[string]interface{})
				res = res && matchQuery(qLogger, expr, doc)
			}
			ok = res
		default:
			// TODO(ts) make this part cleaner
			// (check orignal doc VS flattend doc)
			val, check := doc[key]
			oval, err := maputil.GetPath(key, odoc)
			if !check && err != nil {
				logger.Debug("key not found")
				return false
			}
			// `val` (the value of the queried doc) must be:
			// - a standard type: nil, int, float64, string, bool
			// - a []interface[}
			// It can't ba `map[string]interface{}` since maps are flattened
			switch vval := oval.(type) {
			// If it's an array, the doc is returned if a lest one of the doc match the query
			case []interface{}:
				res := false
				for _, li := range vval {
					res = res || matchQueryValue(eval, li)
				}
				if res {
					ok = ok && true
				}
			default:
				ok = ok && matchQueryValue(eval, val)
			}
		}
		// logger.Debug("subquery res", "ok", ok, "key", key, "eval", eval)
	}
	return ok
}

// matchQueryValue check the query value againt the doc (doc[queried_key]) and compare.
// /!\ doc must be flattened
func matchQueryValue(eval, val interface{}) bool {
	ok := true
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
	return ok
}
