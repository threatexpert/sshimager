package main

import (
	"fmt"
	"os"
	"time"
)

const Version = "1.7.1"

var (
	lastProgressTime    time.Time
	lastProgressWritten uint64
	instantSpeed        float64 // MB/s, smoothed
)

func printProgress(done, total, dataWritten uint64, tStart time.Time) {
	now := time.Now()
	dt := now.Sub(lastProgressTime).Seconds()
	if dt < 1.0 {
		return // throttle to 1Hz
	}

	// Instantaneous speed from bytes written since last update
	if dt > 0 {
		delta := dataWritten - lastProgressWritten
		cur := float64(delta) / dt / 1048576
		if instantSpeed == 0 {
			instantSpeed = cur
		} else {
			// Exponential moving average (α=0.3) for smooth display
			instantSpeed = instantSpeed*0.7 + cur*0.3
		}
	}
	lastProgressTime = now
	lastProgressWritten = dataWritten

	pct := float64(0)
	if total > 0 {
		pct = float64(done) / float64(total) * 100
	}

	// ETA based on instantaneous speed
	eta := ""
	if instantSpeed > 0.1 && pct > 0 && pct < 100 {
		remain := float64(total-done) / (instantSpeed * 1048576)
		if remain < 60 {
			eta = fmt.Sprintf(" ETA %ds", int(remain))
		} else if remain < 3600 {
			eta = fmt.Sprintf(" ETA %dm%ds", int(remain)/60, int(remain)%60)
		} else {
			eta = fmt.Sprintf(" ETA %dh%dm", int(remain)/3600, (int(remain)%3600)/60)
		}
	}

	fmt.Fprintf(os.Stderr, "\r  %s / %s  (%.1f%%)  %.0f MB/s  data: %s%s    ",
		FormatSize(done), FormatSize(total), pct, instantSpeed,
		FormatSize(dataWritten), eta)
}

// ResetProgress resets the instantaneous speed tracker (call after reconnect).
func ResetProgress() {
	lastProgressTime = time.Time{}
	lastProgressWritten = 0
	instantSpeed = 0
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
