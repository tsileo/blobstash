package test

import (
	"fmt"
	"os/exec"
)

func Diff(path1, path2 string) error {
	cmd := exec.Command("diff", "-rq", path1, path2)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("Error running diff: %v, %s", err, string(out))
	}
	return nil
}
