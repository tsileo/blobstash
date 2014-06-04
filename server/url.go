package server

import (
	"log"
	neturl "net/url"
)

// ParseURL converts db url to usable data
// Example:
// local:///path/to/dir
// s3://db
func ParseURL(url string) error {
	u, err := neturl.Parse("local:///my/path")
	if err != nil {
		return err
	}
	log.Printf("%+v\n", u.Scheme)
	log.Printf("%+v\n", u.Host)
	log.Printf("%+v\n", u.Path)
	return nil
}
