package gluarequire2

import (
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/yuin/gopher-lua"
)

var (
	namePrefix = "github.com/"
	// TODO(tsileo): support versioning (@hash, master by default)
	gitHubFileURL = "https://raw.githubusercontent.com/%s/%s/master/%s"
	defaultPath   = "/tmp/gluarequire2"
)

type RequireFromGitHubConf struct {
	Path string
}

type RequireFromGitHub struct {
	conf *RequireFromGitHubConf
	path string
}

func NewRequireFromGitHub(conf *RequireFromGitHubConf) *RequireFromGitHub {
	path := defaultPath
	if conf != nil && conf.Path != "" {
		path = conf.Path
	}
	res := &RequireFromGitHub{conf, path}

	os.MkdirAll(res.path, 0700)

	return res
}

func (rfgh *RequireFromGitHub) Setup(L *lua.LState, name string) (string, error) {
	if !strings.HasPrefix(name, namePrefix) {
		return "", fmt.Errorf("module is not hosted on GitHub")
	}

	path := L.GetField(L.GetField(L.Get(lua.EnvironIndex), "package"), "path").(lua.LString)
	path = lua.LString(rfgh.path + "/?.lua;" + string(path))
	L.SetField(L.GetField(L.Get(lua.EnvironIndex), "package"), "path", lua.LString(path))

	data := strings.Split(name, "/")
	if !(len(data) >= 4) {
		return "", fmt.Errorf("bad format, should be github.com/user/repo/path/to/module")
	}
	hpath := fmt.Sprintf("%x", sha1.Sum([]byte(name)))
	os.Mkdir(filepath.Join(rfgh.path, hpath), 0700)
	newName := filepath.Join(hpath, data[len(data)-1])
	modpath := newName + ".lua"
	modloc := filepath.Join(rfgh.path, modpath)

	if _, err := os.Stat(modloc); err == nil {
		return newName, nil
	}

	username := data[1]
	repo := data[2]

	url := fmt.Sprintf(gitHubFileURL, username, repo, strings.Join(data[3:], "/")) + ".lua"

	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to fetch module: %s", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %s", err)
	}

	if err := ioutil.WriteFile(modloc, body, 0644); err != nil {
		return "", fmt.Errorf("failed to write downloaded module: %s", err)
	}

	return newName, nil
}
