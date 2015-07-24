package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// GetDebug fetch the expvar debug variables from the server
func GetDebug() (map[string]interface{}, error) {
	res := make(map[string]interface{})
	request, err := http.NewRequest("GET", "http://0.0.0.0:9736/debug/vars", nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
			return res, err
		}
		return res, nil
	default:
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("failed to fetch debug: %v", body)
	}
}
