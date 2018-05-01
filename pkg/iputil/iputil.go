// Package iputil implements IP address related utils.
package iputil // import "a4.io/blobstash/pkg/iputil"
import (
	"net"
	"net/url"
	"strings"
)

var privateIPNets [3]*net.IPNet

func init() {
	var err error
	for i, cidr := range []string{"192.168.0.0/16", "10.0.0.0/8", "176.16.0.0/12"} {
		_, privateIPNets[i], err = net.ParseCIDR(cidr)
		if err != nil {
			panic(err)
		}
	}
}

// IsIPPrivate retrurns true if the given IP address is part of a private network
func IsIPPrivate(ip net.IP) bool {
	for _, ipnet := range privateIPNets {
		if ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

// IsPrivate returns true if the given host revolve to a private IP address (or if a private address is passed)
func IsPrivate(host string) (bool, error) {
	if strings.HasPrefix(host, "http") {
		u, err := url.Parse(host)
		if err != nil {
			return false, err
		}
		host = u.Hostname()
	}
	if ip := net.ParseIP(host); ip != nil {
		return IsIPPrivate(ip), nil
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return false, err
	}
	if len(ips) == 0 {
		return false, err
	}
	for _, ip := range ips {
		if !IsIPPrivate(ip) {
			return false, nil
		}
	}
	return true, nil
}
