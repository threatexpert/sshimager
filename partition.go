package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// FSType represents detected filesystem type
type FSType int

const (
	FSUnknown FSType = iota
	FSExt2
	FSExt3
	FSExt4
	FSXFS
	FSBtrfs
	FSLVM
	FSSwap
	FSFat32
	FSNTFS
)

func (f FSType) String() string {
	switch f {
	case FSExt2:
		return "ext2"
	case FSExt3:
		return "ext3"
	case FSExt4:
		return "ext4"
	case FSXFS:
		return "xfs"
	case FSBtrfs:
		return "btrfs"
	case FSLVM:
		return "lvm"
	case FSSwap:
		return "swap"
	case FSFat32:
		return "fat32"
	case FSNTFS:
		return "ntfs"
	}
	return "unknown"
}

func (f FSType) SupportsBitmap() bool {
	return f == FSExt2 || f == FSExt3 || f == FSExt4 || f == FSXFS || f == FSLVM || f == FSSwap
}

// PartTable type
type PartTableType int

const (
	PTNone PartTableType = iota
	PTMBR
	PTGPT
)

// CopyMode for each partition
type CopyMode int

const (
	CopyFull     CopyMode = iota // Full sector-by-sector copy
	CopyUsedOnly                 // Only used blocks (bitmap-aware)
	CopySkip                     // Exclude from image
)

// PartitionInfo describes a single partition
type PartitionInfo struct {
	Number     int
	DevPath    string // e.g. /dev/sda1
	Offset     uint64
	Size       uint64
	FSType     FSType
	FSLabel    string
	Mountpoint string
	CopyMode   CopyMode
}

// DiskInfo describes a remote disk
type DiskInfo struct {
	DevPath    string
	Model      string
	Size       uint64
	SectorSize uint32
	PTType     PartTableType
	Partitions []PartitionInfo
}

const maxPartitions = 128

// ReadAt helper: read exactly len(buf) bytes from r at offset
func readAt(r io.ReaderAt, buf []byte, offset int64) error {
	n, err := r.ReadAt(buf, offset)
	if n == len(buf) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("short read: got %d, want %d", n, len(buf))
}

// ScanPartitions reads and parses the partition table from a ReaderAt
func ScanPartitions(r io.ReaderAt, diskSize uint64, devPath string) (*DiskInfo, error) {
	info := &DiskInfo{
		DevPath:    devPath,
		Size:       diskSize,
		SectorSize: 512,
	}

	// Read first 34 sectors (MBR + GPT header + some GPT entries)
	buf := make([]byte, 512*34)
	n, _ := r.ReadAt(buf, 0)
	if n < 512 {
		return nil, fmt.Errorf("cannot read disk header")
	}

	// Try GPT first (LBA 1)
	if n >= 1024 {
		if err := parseGPT(r, buf[512:1024], info, devPath); err == nil {
			detectAllFS(r, info)
			return info, nil
		}
	}

	// Fall back to MBR
	if err := parseMBR(r, buf[:512], info, devPath); err != nil {
		// No partition table found.
		// Check if the whole disk has a filesystem directly (e.g. /dev/sda formatted as ext4)
		fsType, fsLabel := DetectFS(r, 0)
		if fsType != FSUnknown {
			info.PTType = PTNone
			info.Partitions = append(info.Partitions, PartitionInfo{
				Number:   0,
				DevPath:  devPath, // whole disk device = partition device
				Offset:   0,
				Size:     diskSize,
				FSType:   fsType,
				FSLabel:  fsLabel,
				CopyMode: CopyFull,
			})
		}
		return info, nil
	}
	detectAllFS(r, info)
	return info, nil
}

func isZeroGUID(guid []byte) bool {
	for _, b := range guid {
		if b != 0 {
			return false
		}
	}
	return true
}

func needsPartSep(devPath string) bool {
	base := devPath
	if idx := strings.LastIndex(devPath, "/"); idx >= 0 {
		base = devPath[idx+1:]
	}
	return strings.HasPrefix(base, "nvme") || strings.HasPrefix(base, "mmcblk")
}

func partDevPath(devPath string, num int) string {
	if needsPartSep(devPath) {
		return fmt.Sprintf("%sp%d", devPath, num)
	}
	return fmt.Sprintf("%s%d", devPath, num)
}

func parseGPT(r io.ReaderAt, lba1 []byte, info *DiskInfo, devPath string) error {
	if string(lba1[0:8]) != "EFI PART" {
		return fmt.Errorf("not GPT")
	}
	info.PTType = PTGPT

	entryLBA := binary.LittleEndian.Uint64(lba1[72:80])
	numEntries := binary.LittleEndian.Uint32(lba1[80:84])
	entrySize := binary.LittleEndian.Uint32(lba1[84:88])
	if numEntries > maxPartitions {
		numEntries = maxPartitions
	}
	if entrySize < 128 {
		entrySize = 128
	}

	tblSize := int(numEntries) * int(entrySize)
	tbl := make([]byte, tblSize)
	if err := readAt(r, tbl, int64(entryLBA)*512); err != nil {
		return err
	}

	for i := uint32(0); i < numEntries; i++ {
		off := int(i) * int(entrySize)
		entry := tbl[off : off+int(entrySize)]

		if isZeroGUID(entry[0:16]) {
			continue
		}
		startLBA := binary.LittleEndian.Uint64(entry[32:40])
		endLBA := binary.LittleEndian.Uint64(entry[40:48])
		if startLBA == 0 || endLBA == 0 {
			continue
		}

		p := PartitionInfo{
			Number:   int(i) + 1,
			DevPath:  partDevPath(devPath, int(i)+1),
			Offset:   startLBA * 512,
			Size:     (endLBA - startLBA + 1) * 512,
			CopyMode: CopyFull,
		}
		info.Partitions = append(info.Partitions, p)
	}
	return nil
}

func parseMBR(r io.ReaderAt, sector0 []byte, info *DiskInfo, devPath string) error {
	if sector0[510] != 0x55 || sector0[511] != 0xAA {
		return fmt.Errorf("no MBR signature")
	}
	info.PTType = PTMBR

	var ebrBase uint64

	// Primary partitions
	for i := 0; i < 4; i++ {
		off := 446 + i*16
		ptype := sector0[off+4]
		lbaStart := binary.LittleEndian.Uint32(sector0[off+8 : off+12])
		lbaCount := binary.LittleEndian.Uint32(sector0[off+12 : off+16])
		if ptype == 0 || lbaCount == 0 {
			continue
		}
		if ptype == 0x05 || ptype == 0x0F || ptype == 0x85 {
			ebrBase = uint64(lbaStart)
			continue
		}
		p := PartitionInfo{
			Number:   i + 1,
			DevPath:  partDevPath(devPath, i+1),
			Offset:   uint64(lbaStart) * 512,
			Size:     uint64(lbaCount) * 512,
			CopyMode: CopyFull,
		}
		info.Partitions = append(info.Partitions, p)
	}

	// EBR chain for logical partitions
	if ebrBase > 0 {
		ebrLBA := ebrBase
		logicalNum := 5
		for ebrLBA > 0 && len(info.Partitions) < maxPartitions {
			ebr := make([]byte, 512)
			if err := readAt(r, ebr, int64(ebrLBA)*512); err != nil {
				break
			}
			if ebr[510] != 0x55 || ebr[511] != 0xAA {
				break
			}
			// Entry 0: the logical partition
			ptype := ebr[446+4]
			lbaStart := binary.LittleEndian.Uint32(ebr[446+8 : 446+12])
			lbaCount := binary.LittleEndian.Uint32(ebr[446+12 : 446+16])
			if ptype != 0 && lbaCount > 0 {
				partLBA := ebrLBA + uint64(lbaStart)
				p := PartitionInfo{
					Number:   logicalNum,
					DevPath:  partDevPath(devPath, logicalNum),
					Offset:   partLBA * 512,
					Size:     uint64(lbaCount) * 512,
					CopyMode: CopyFull,
				}
				info.Partitions = append(info.Partitions, p)
				logicalNum++
			}
			// Entry 1: next EBR
			nextType := ebr[446+16+4]
			nextLBA := binary.LittleEndian.Uint32(ebr[446+16+8 : 446+16+12])
			if nextType != 0 && nextLBA > 0 {
				ebrLBA = ebrBase + uint64(nextLBA)
			} else {
				ebrLBA = 0
			}
		}
	}
	return nil
}

// DetectFS detects the filesystem type of a partition by reading magic bytes
func DetectFS(r io.ReaderAt, partOffset uint64) (FSType, string) {
	buf := make([]byte, 4096)
	if err := readAt(r, buf, int64(partOffset)); err != nil {
		return FSUnknown, ""
	}

	// ext2/3/4: magic 0xEF53 at offset 1024+56
	if len(buf) >= 1082 {
		magic := binary.LittleEndian.Uint16(buf[1024+56:])
		if magic == 0xEF53 {
			incompat := binary.LittleEndian.Uint32(buf[1024+0x60:])
			compat := binary.LittleEndian.Uint32(buf[1024+0x5C:])
			label := strings.TrimRight(string(buf[1024+120:1024+136]), "\x00")
			if incompat&0x0040 != 0 {
				return FSExt4, label
			}
			if compat&0x0004 != 0 {
				return FSExt3, label
			}
			return FSExt2, label
		}
	}

	// XFS: "XFSB" at offset 0
	if string(buf[0:4]) == "XFSB" {
		label := strings.TrimRight(string(buf[108:120]), "\x00")
		return FSXFS, label
	}

	// Btrfs: magic at offset 0x10040
	btrfsBuf := make([]byte, 16)
	if readAt(r, btrfsBuf, int64(partOffset)+0x10040) == nil {
		if string(btrfsBuf[0:8]) == "_BHRfS_M" {
			return FSBtrfs, ""
		}
	}

	// LVM: "LABELONE" at offset 512
	lvmBuf := make([]byte, 512)
	if readAt(r, lvmBuf, int64(partOffset)+512) == nil {
		if string(lvmBuf[0:8]) == "LABELONE" {
			return FSLVM, ""
		}
	}

	// Swap: "SWAPSPACE2" or "SWAP-SPACE" at offset 4086
	swBuf := make([]byte, 10)
	if readAt(r, swBuf, int64(partOffset)+4086) == nil {
		sig := string(swBuf)
		if sig == "SWAPSPACE2" || sig == "SWAP-SPACE" {
			return FSSwap, ""
		}
	}

	// FAT32
	if buf[510] == 0x55 && buf[511] == 0xAA && string(buf[82:90]) == "FAT32   " {
		return FSFat32, ""
	}

	// NTFS
	if string(buf[3:7]) == "NTFS" {
		return FSNTFS, ""
	}

	return FSUnknown, ""
}

func detectAllFS(r io.ReaderAt, info *DiskInfo) {
	for i := range info.Partitions {
		info.Partitions[i].FSType, info.Partitions[i].FSLabel = DetectFS(r, info.Partitions[i].Offset)
	}
}
