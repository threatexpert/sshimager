//go:build !linux

package main

func redirectStderr() {
	// no-op on non-Linux (agent only runs on Linux)
}
