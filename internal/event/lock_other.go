//go:build !linux && !darwin

package event

import (
	"errors"
	"os"
)

func lockFile(_ *os.File) error {
	return errors.New("event: file ownership locking requires Linux or macOS")
}
