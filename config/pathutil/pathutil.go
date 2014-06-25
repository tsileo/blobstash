/*

Package pathutil implements helpers to retrieve config/cache/var directories.

Follow XDG Base Directory Specification:
http://standards.freedesktop.org/basedir-spec/basedir-spec-0.6.html

*/
package pathutil

import (
	"os"
	"path/filepath"
)

// configDir find the best config directory
// following XDG Base Directory Specification
// http://standards.freedesktop.org/basedir-spec/basedir-spec-0.6.html
// These environment variable are checked in this order:
// - $BLOBSTASH_CONFIG_DIR
// - $XDG_CONFIG_HOME
// And will fallback to:
// - $HOME/.config/
func ConfigDir() string {
	if dir := os.Getenv("BLOBSTASH_CONFIG_DIR"); dir != "" {
		return dir
	}
	if dir := os.Getenv("XDG_CONFIG_HOME"); dir != "" {
		return filepath.Join(dir, "blobdb")
	}
	return filepath.Join(os.Getenv("HOME"), ".config", "blobstash")
}

// varDir find the best var directory
// These environment variable are checked in this order:
// - $BLOBSTASH_CONFIG_DIR
// And will fallback to:
// - $HOME/var/
func VarDir() string {
	if dir := os.Getenv("BLOBSTASH_VAR_DIR"); dir != "" {
		return dir
	}
	return filepath.Join(os.Getenv("HOME"), "var", "blobstash")
}

// cacheDir return current user cache directory
// following XDG Base Directory Specification
// http://standards.freedesktop.org/basedir-spec/basedir-spec-0.6.html
// These environment variable are checked in this order:
// - $BLOBSTASH_CACHE_DIR
// - $XDG_CACHE_HOME
// And will fallback to:
// - $HOME/.cache/
func CacheDir() string {
	if dir := os.Getenv("BLOBSTASH_CACHE_DIR"); dir != "" {
		return dir
	}
	if dir := os.Getenv("XDG_CACHE_HOME"); dir != "" {
		return filepath.Join(dir, "blobstash")
	}
	return filepath.Join(os.Getenv("HOME"), ".cache", "blobstash")
}
