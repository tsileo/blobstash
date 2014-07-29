package script

import (
	"net/http"
	"bytes"
	"encoding/json"
	"github.com/tsileo/blobstash/client"
)

var defaultServerAddr = "http://localhost:9736"

func RunScript(serverAddr, code string, args interface{}) (map[string]interface{}, error) {
	if serverAddr == "" {
		serverAddr = defaultServerAddr
	}
	body := &bytes.Buffer
	// => make the json
	request, err := http.NewRequest("POST", serverAddr+"/scripting", body)
	if err != nil {
		return nil, err
	}
	client :=  http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	res := map[string]interface{}{}
	decoder := json.NewDecoder(resp.Body)
    if err := decoder.Decode(&res); err != nil {
        return nil, err
    }
	defer resp.Body.Close()
	// DECODE JSON
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to run script")
	}
	return res, nil
}
