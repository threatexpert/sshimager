//go:build windows
// +build windows

package main

import (
	"os"

	"golang.org/x/sys/windows"
)

func initTerminal() {
	// Enable ANSI escape sequence processing on Windows console
	handles := []windows.Handle{
		windows.Handle(os.Stdout.Fd()),
		windows.Handle(os.Stderr.Fd()),
	}
	for _, h := range handles {
		var mode uint32
		if err := windows.GetConsoleMode(h, &mode); err == nil {
			if mode&windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING == 0 {
				windows.SetConsoleMode(h, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING)
			}
		}
	}
}
