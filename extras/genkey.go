package main

import (
	"fmt"
	"io/ioutil"

	"code.google.com/p/go.crypto/scrypt"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	var pass, salt string
	fmt.Printf("pass:")
	fmt.Scanf("%v", &pass)
	fmt.Printf("salt:")
	fmt.Scanf("%v", &salt)
	key, err := scrypt.Key([]byte(pass), []byte(salt), 16384, 8, 1, 32)
	check(err)
	err = ioutil.WriteFile("blobstash.key", key, 0644)
	check(err)
}
