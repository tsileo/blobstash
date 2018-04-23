package iputil

import "testing"

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestIsPrivate(t *testing.T) {
	for _, data := range []struct {
		host     string
		expected bool
	}{
		{"192.168.1.100", true},
		{"8.8.8.8", false},
		{"google.com", false},
		{"10.0.0.5", true},
		{"176.16.0.1", true},
	} {
		res, err := IsPrivate(data.host)
		check(err)
		if res != data.expected {
			t.Errorf("IsPrivate(%q) failed, expected %q, got %q", data.host, data.expected, res)
		}
	}
}
