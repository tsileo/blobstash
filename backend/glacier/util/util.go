package util

import (
	"os"
    "path/filepath"

	"github.com/cznic/kv"
)

func opts() *kv.Options {
    return &kv.Options{
        VerifyDbBeforeOpen:  true,
        VerifyDbAfterOpen:   true,
        VerifyDbBeforeClose: true,
        VerifyDbAfterClose:  true,
    }
}

var (
    XDGDataHome = filepath.Join(os.Getenv("HOME"), ".local/share")
    DBPath = filepath.Join(XDGDataHome, "datadb-glacier-db")
)

func GetDB() (*kv.DB, error) {
    if err := os.MkdirAll(XDGDataHome, 0700); err != nil {
        return nil, err
    }
    createOpen := kv.Open
    if _, err := os.Stat(DBPath); os.IsNotExist(err) {
        createOpen = kv.Create
    }
    db, err := createOpen(DBPath, opts())
    if err != nil {
        return nil, err
    }
    return db, nil
}
