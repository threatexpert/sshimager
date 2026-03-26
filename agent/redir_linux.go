package main

import (
	"os"
	"syscall"
)

func redirectStderr() {
	devNull, err := os.OpenFile("/dev/null", os.O_WRONLY, 0)
	if err == nil {
		// Use Dup3 with flags=0, which is equivalent to Dup2 and available on all Linux archs
		syscall.Dup3(int(devNull.Fd()), 2, 0)
		devNull.Close()
	}
}
