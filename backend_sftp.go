package main

import (
	"fmt"
	"os"
)

// SFTPBackend implements DiskBackend using the standard SFTP protocol.
// This wraps the existing SSHConn with bitmap parsing done client-side.
type SFTPBackend struct {
	conn *SSHConn
}

// NewSFTPBackend creates a new SFTP-based disk backend.
// The SSHConn must already have a disk opened via OpenDisk().
func NewSFTPBackend(conn *SSHConn) *SFTPBackend {
	return &SFTPBackend{conn: conn}
}

func (b *SFTPBackend) ReadAt(p []byte, off int64) (int, error) {
	return b.conn.ReadAt(p, off)
}

func (b *SFTPBackend) GetBitmap(partOffset, partSize uint64, fsType FSType, devPath string) (*BlockBitmap, error) {
	switch fsType {
	case FSExt2, FSExt3, FSExt4:
		return Ext4ReadBitmap(b.conn, partOffset, partSize)
	case FSXFS:
		return XFSReadBitmap(b.conn, partOffset, partSize)
	case FSLVM:
		return LVMBuildBitmap(b.conn, partOffset, partSize, devPath)
	case FSSwap:
		// Swap has no meaningful bitmap; caller handles this case
		return nil, fmt.Errorf("swap partitions have no bitmap")
	default:
		return nil, fmt.Errorf("filesystem %s does not support bitmap", fsType)
	}
}

func (b *SFTPBackend) Reconnect() error {
	fmt.Fprintf(os.Stderr, "\nReconnecting (SFTP backend)...\n")
	if err := b.conn.Reconnect(); err != nil {
		return err
	}
	// Re-setup SFTP after SSH reconnect
	if err := b.conn.SetupSFTP(); err != nil {
		return fmt.Errorf("SFTP setup on reconnect: %w", err)
	}
	// Re-open disk device
	if b.conn.DevPath != "" {
		if err := b.conn.OpenDisk(b.conn.DevPath); err != nil {
			return fmt.Errorf("reopen disk on reconnect: %w", err)
		}
	}
	return nil
}

func (b *SFTPBackend) IsNetworkError(err error) bool {
	return IsNetworkError(err)
}

func (b *SFTPBackend) RemoteInfo() (user, host string) {
	return b.conn.User, b.conn.Host
}

func (b *SFTPBackend) Close() {
	b.conn.Close()
}

// Conn returns the underlying SSHConn for operations that still
// need direct SSH access (disk discovery, partition scanning, mounts).
// These are pre-imaging setup steps that run before the backend is
// used for data transfer.
func (b *SFTPBackend) Conn() *SSHConn {
	return b.conn
}
