//go:build !linux && !darwin

package messagebuilder

import (
	"errors"
	"os"
)

func lockFile(_ *os.File) error {
	return errors.New("messagebuilder: file ownership locking requires Linux or macOS")
}
