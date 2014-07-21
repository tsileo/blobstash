package client2

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/garyburd/redigo/redis"
)

var tag = "blobdb"

func ToSlice(in interface{}) ([]string, error) {
	out := []string{}

	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("ToSlice only accepts structs; got %T", v)
	}

	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		fi := typ.Field(i)
		if tagv := fi.Tag.Get(tag); tagv != "" && tagv != "-" {
			switch v.Field(i).Kind() {
			case reflect.Bool:
				out = append(out, tagv, strconv.FormatBool(v.Field(i).Bool()))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				out = append(out, tagv, strconv.Itoa(int(v.Field(i).Int())))
			case reflect.String:
				out = append(out, tagv, v.Field(i).String())
			default:
				panic(fmt.Sprintf("%v is not a supported type", v.Field(i).Kind()))
			}
		}
	}
	return out, nil
}

func FormatStruct(s interface{}) []string {
	res := []string{}
	args := redis.Args{}.AddFlat(s)
	for _, arg := range args {
		var sarg string
		switch v := arg.(type) {
		case int64:
			sarg = strconv.FormatInt(v, 10)
		case int:
			sarg = strconv.Itoa(v)
		case bool:
			sarg = strconv.FormatBool(v)
		case string:
			sarg = v
		//case float64:
		//	strconv.FormatFloat(v, 'g', -1, 64)
		case []byte:
			sarg = string(v)
		case nil:
			sarg = ""
		default:
			sarg = fmt.Sprintf("%v", v)
		}
		res = append(res, sarg)
	}
	return res
}