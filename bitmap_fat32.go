package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Fat32ReadBitmap reads the FAT32 File Allocation Table to build a used-block bitmap.
// FAT32 layout:
//   Boot Sector (sector 0) → BPB parameters
//   Reserved sectors → includes boot sector + FSInfo
//   FAT tables (1 or 2 copies) → array of 4-byte cluster entries
//   Data region → clusters 2..N
//
// Each FAT entry: 0x00000000 = free, anything else = used (low 28 bits).
func Fat32ReadBitmap(r io.ReaderAt, partOffset, partSize uint64) (*BlockBitmap, error) {
	// Read boot sector (512 bytes)
	bs := make([]byte, 512)
	if err := readAt(r, bs, int64(partOffset)); err != nil {
		return nil, fmt.Errorf("fat32: cannot read boot sector: %w", err)
	}

	// Verify signature
	if bs[510] != 0x55 || bs[511] != 0xAA {
		return nil, fmt.Errorf("fat32: bad boot signature")
	}

	// BPB fields
	bytesPerSec := binary.LittleEndian.Uint16(bs[11:13])
	secPerClus := bs[13]
	rsvdSecCnt := binary.LittleEndian.Uint16(bs[14:16])
	numFATs := bs[16]
	totSec32 := binary.LittleEndian.Uint32(bs[32:36])
	fatSz32 := binary.LittleEndian.Uint32(bs[36:40])

	if bytesPerSec == 0 || secPerClus == 0 || rsvdSecCnt == 0 || numFATs == 0 || fatSz32 == 0 {
		return nil, fmt.Errorf("fat32: invalid BPB parameters")
	}

	// Verify FAT32 signature string (optional but helpful)
	if string(bs[82:90]) != "FAT32   " {
		return nil, fmt.Errorf("fat32: not a FAT32 filesystem")
	}

	clusterSize := uint32(bytesPerSec) * uint32(secPerClus)

	// Data region starts after reserved sectors + all FAT copies
	fatStartSec := uint64(rsvdSecCnt)
	dataStartSec := fatStartSec + uint64(numFATs)*uint64(fatSz32)

	// Total data sectors
	var totalSec uint64
	if totSec32 != 0 {
		totalSec = uint64(totSec32)
	} else {
		// Try 16-bit total sectors field (unlikely for FAT32 but be safe)
		totalSec = uint64(binary.LittleEndian.Uint16(bs[19:21]))
	}
	if totalSec == 0 {
		return nil, fmt.Errorf("fat32: zero total sectors")
	}

	dataSectors := totalSec - dataStartSec
	totalClusters := dataSectors / uint64(secPerClus)

	// We treat each cluster as one "block" in the bitmap.
	// Cluster numbering starts at 2 in FAT, so FAT entries 0,1 are reserved.
	// Our bitmap index 0 = cluster 2 (first data cluster).
	//
	// But for imaging, we need a bitmap over the entire partition.
	// Strategy: use sector-granularity would be too fine. Use cluster as block.
	// Total blocks = (reserved + FAT area as blocks) + data clusters.
	//
	// Simpler: use cluster size as block size, total blocks covers entire partition.
	totalBlocks := partSize / uint64(clusterSize)
	if partSize%uint64(clusterSize) != 0 {
		totalBlocks++
	}

	bitmapBytes := (totalBlocks + 7) / 8
	bm := &BlockBitmap{
		Bits:        make([]byte, bitmapBytes),
		BlockSize:   clusterSize,
		TotalBlocks: totalBlocks,
	}

	// Mark reserved sectors + FAT area as used (blocks 0..dataStartBlock-1)
	dataStartBlock := dataStartSec * uint64(bytesPerSec) / uint64(clusterSize)
	for b := uint64(0); b < dataStartBlock && b < totalBlocks; b++ {
		bm.Bits[b/8] |= 1 << (b % 8)
	}

	// Read the FAT table
	fatOffset := partOffset + fatStartSec*uint64(bytesPerSec)
	fatSize := uint64(fatSz32) * uint64(bytesPerSec)
	fat := make([]byte, fatSize)
	if err := readAt(r, fat, int64(fatOffset)); err != nil {
		return nil, fmt.Errorf("fat32: cannot read FAT: %w", err)
	}

	// Walk FAT entries: entry 0,1 are reserved, entries 2..totalClusters+1 are data clusters
	for i := uint64(0); i < totalClusters; i++ {
		fatIdx := i + 2 // FAT entry index (clusters start at 2)
		entryOff := fatIdx * 4
		if entryOff+4 > uint64(len(fat)) {
			break
		}
		entry := binary.LittleEndian.Uint32(fat[entryOff:entryOff+4]) & 0x0FFFFFFF
		if entry != 0 {
			// Used cluster — map to bitmap block
			block := dataStartBlock + i
			if block < totalBlocks {
				bm.Bits[block/8] |= 1 << (block % 8)
			}
		}
	}

	return bm, nil
}
