//go:build !linux && !darwin

package brain

import (
	"errors"
	"os"
)

func lockFile(_ *os.File) error {
	return errors.New("brain: file ownership locking requires Linux or macOS")
}
