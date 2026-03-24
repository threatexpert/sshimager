package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// VDiskFormat represents output image format
type VDiskFormat int

const (
	FormatVMDK VDiskFormat = iota
	FormatVHD
	FormatVDI
	FormatDD
)

func (f VDiskFormat) String() string {
	switch f {
	case FormatVMDK:
		return "VMDK"
	case FormatVHD:
		return "VHD"
	case FormatVDI:
		return "VDI"
	case FormatDD:
		return "DD"
	}
	return "unknown"
}

func FormatFromExt(path string) VDiskFormat {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".vmdk":
		return FormatVMDK
	case ".vhd":
		return FormatVHD
	case ".vdi":
		return FormatVDI
	case ".dd", ".raw", ".img":
		return FormatDD
	}
	return FormatVMDK // default
}

func FormatFromName(name string) VDiskFormat {
	switch strings.ToLower(name) {
	case "vmdk":
		return FormatVMDK
	case "vhd":
		return FormatVHD
	case "vdi":
		return FormatVDI
	case "dd", "raw":
		return FormatDD
	}
	return FormatVMDK
}

// VDiskWriter is the interface all format writers implement
type VDiskWriter interface {
	// Write data at the given virtual disk offset
	Write(offset uint64, data []byte) error
	// WriteZero marks a region as zero (sparse if format supports it)
	WriteZero(offset uint64, length uint64) error
	// Close finalizes and closes the image
	Close() error
}

// CreateVDisk creates a new virtual disk writer
func CreateVDisk(path string, format VDiskFormat, diskSize uint64) (VDiskWriter, error) {
	switch format {
	case FormatVMDK:
		return NewVMDKWriter(path, diskSize)
	case FormatVHD:
		return NewVHDWriter(path, diskSize)
	case FormatVDI:
		return NewVDIWriter(path, diskSize)
	case FormatDD:
		return NewDDWriter(path, diskSize)
	}
	return nil, fmt.Errorf("unsupported format: %v", format)
}

// ---- DD/RAW Writer (simplest) ----

type DDWriter struct {
	file     *os.File
	diskSize uint64
}

func NewDDWriter(path string, diskSize uint64) (*DDWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	// Pre-extend file
	if err := f.Truncate(int64(diskSize)); err != nil {
		f.Close()
		return nil, err
	}
	return &DDWriter{file: f, diskSize: diskSize}, nil
}

func (w *DDWriter) Write(offset uint64, data []byte) error {
	_, err := w.file.WriteAt(data, int64(offset))
	return err
}

func (w *DDWriter) WriteZero(offset uint64, length uint64) error {
	// File is already zero-filled from Truncate
	return nil
}

func (w *DDWriter) Close() error {
	return w.file.Close()
}
