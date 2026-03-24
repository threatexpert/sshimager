package main

import (
	"fmt"
	"os"
	"time"
)

const Version = "1.6.0"

var lastProgressTime time.Time

func printProgress(done, total, dataWritten uint64, tStart time.Time) {
	now := time.Now()
	if now.Sub(lastProgressTime) < time.Second {
		return // throttle to 1Hz
	}
	lastProgressTime = now

	elapsed := now.Sub(tStart).Seconds()
	speed := float64(0)
	if elapsed > 0.5 {
		speed = float64(dataWritten) / elapsed / 1048576
	}
	pct := float64(0)
	if total > 0 {
		pct = float64(done) / float64(total) * 100
	}

	// ETA
	eta := ""
	if speed > 0 && pct > 0 && pct < 100 {
		remain := float64(total-done) / (speed * 1048576)
		if remain < 60 {
			eta = fmt.Sprintf(" ETA %ds", int(remain))
		} else if remain < 3600 {
			eta = fmt.Sprintf(" ETA %dm%ds", int(remain)/60, int(remain)%60)
		} else {
			eta = fmt.Sprintf(" ETA %dh%dm", int(remain)/3600, (int(remain)%3600)/60)
		}
	}

	fmt.Fprintf(os.Stderr, "\r  %s / %s  (%.1f%%)  %.0f MB/s  data: %s%s    ",
		FormatSize(done), FormatSize(total), pct, speed,
		FormatSize(dataWritten), eta)
}

func FormatSize(bytes uint64) string {
	switch {
	case bytes >= 1000*1000*1000*1000:
		return fmt.Sprintf("%.2f TB", float64(bytes)/(1000*1000*1000*1000))
	case bytes >= 1000*1000*1000:
		return fmt.Sprintf("%.2f GB", float64(bytes)/(1000*1000*1000))
	case bytes >= 1000*1000:
		return fmt.Sprintf("%.2f MB", float64(bytes)/(1000*1000))
	case bytes >= 1000:
		return fmt.Sprintf("%.2f KB", float64(bytes)/1000)
	}
	return fmt.Sprintf("%d B", bytes)
}
