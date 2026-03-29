package main

import "sshimager/bitmap"

// DiskBackend abstracts the remote disk access layer.
// Two implementations exist:
//   - SFTPBackend: uses standard sftp-server (existing behavior)
//   - AgentBackend: uses a custom agent binary (Phase 2)
//
// The upper layer (imaging, TUI, vdisk writers) operates only through
// this interface, making the transport mechanism swappable.
type DiskBackend interface {
	// ReadAt reads len(p) bytes from the remote disk at the given offset.
	// Implements io.ReaderAt semantics.
	ReadAt(p []byte, off int64) (int, error)

	// GetBitmap returns the block allocation bitmap for a partition.
	// For SFTPBackend this reads the bitmap locally over SFTP.
	// For AgentBackend this is computed server-side and sent compressed.
	GetBitmap(partOffset, partSize uint64, fsType FSType, devPath string) (*bitmap.BlockBitmap, error)

	// Reconnect re-establishes the connection using saved credentials.
	// Returns nil on success. The caller may retry reads after reconnect.
	Reconnect() error

	// IsNetworkError returns true if the error indicates a network
	// disconnection that can be resolved by Reconnect().
	IsNetworkError(err error) bool

	// RemoteInfo returns user@host string for display/logging purposes.
	RemoteInfo() (user, host string)

	// Close releases all resources.
	Close()
}
