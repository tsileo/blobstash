package script

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

var defaultServerAddr = "http://localhost:9736"

func RunScript(serverAddr, code string, args interface{}, dest interface{}) (error) {
	if serverAddr == "" {
		serverAddr = defaultServerAddr
	}
	body := &bytes.Buffer{}
	payload := map[string]interface{}{
		"_args":   args,
		"_script": code,
	}
	js, err := json.Marshal(&payload)
	if err != nil {
		return err
	}
	body.Write(js)
	request, err := http.NewRequest("POST", serverAddr+"/scripting", body)
	if err != nil {
		return fmt.Errorf("failed to POST script: %v", err)
	}
	client := http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&dest); err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to run script")
	}
	return nil
}
