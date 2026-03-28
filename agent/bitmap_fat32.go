package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

func readFat32Bitmap(r io.ReaderAt, partOffset, partSize uint64) ([]byte, uint32, uint64, error) {
	bs := make([]byte, 512)
	if err := readFullAt(r, bs, int64(partOffset)); err != nil {
		return nil, 0, 0, fmt.Errorf("fat32: cannot read boot sector: %w", err)
	}

	if bs[510] != 0x55 || bs[511] != 0xAA {
		return nil, 0, 0, fmt.Errorf("fat32: bad boot signature")
	}
	if string(bs[82:90]) != "FAT32   " {
		return nil, 0, 0, fmt.Errorf("fat32: not a FAT32 filesystem")
	}

	bytesPerSec := binary.LittleEndian.Uint16(bs[11:13])
	secPerClus := bs[13]
	rsvdSecCnt := binary.LittleEndian.Uint16(bs[14:16])
	numFATs := bs[16]
	totSec32 := binary.LittleEndian.Uint32(bs[32:36])
	fatSz32 := binary.LittleEndian.Uint32(bs[36:40])

	if bytesPerSec == 0 || secPerClus == 0 || rsvdSecCnt == 0 || numFATs == 0 || fatSz32 == 0 {
		return nil, 0, 0, fmt.Errorf("fat32: invalid BPB parameters")
	}

	clusterSize := uint32(bytesPerSec) * uint32(secPerClus)
	fatStartSec := uint64(rsvdSecCnt)
	dataStartSec := fatStartSec + uint64(numFATs)*uint64(fatSz32)

	var totalSec uint64
	if totSec32 != 0 {
		totalSec = uint64(totSec32)
	} else {
		totalSec = uint64(binary.LittleEndian.Uint16(bs[19:21]))
	}
	if totalSec == 0 {
		return nil, 0, 0, fmt.Errorf("fat32: zero total sectors")
	}

	dataSectors := totalSec - dataStartSec
	totalClusters := dataSectors / uint64(secPerClus)

	totalBlocks := partSize / uint64(clusterSize)
	if partSize%uint64(clusterSize) != 0 {
		totalBlocks++
	}

	bitmapBytes := (totalBlocks + 7) / 8
	bits := make([]byte, bitmapBytes)

	// Mark reserved + FAT area as used
	dataStartBlock := dataStartSec * uint64(bytesPerSec) / uint64(clusterSize)
	for b := uint64(0); b < dataStartBlock && b < totalBlocks; b++ {
		bits[b/8] |= 1 << (b % 8)
	}

	// Read FAT table
	fatOffset := partOffset + fatStartSec*uint64(bytesPerSec)
	fatSize := uint64(fatSz32) * uint64(bytesPerSec)
	fat := make([]byte, fatSize)
	if err := readFullAt(r, fat, int64(fatOffset)); err != nil {
		return nil, 0, 0, fmt.Errorf("fat32: cannot read FAT: %w", err)
	}

	// Walk FAT entries
	for i := uint64(0); i < totalClusters; i++ {
		fatIdx := i + 2
		entryOff := fatIdx * 4
		if entryOff+4 > uint64(len(fat)) {
			break
		}
		entry := binary.LittleEndian.Uint32(fat[entryOff:entryOff+4]) & 0x0FFFFFFF
		if entry != 0 {
			block := dataStartBlock + i
			if block < totalBlocks {
				bits[block/8] |= 1 << (block % 8)
			}
		}
	}

	return bits, clusterSize, totalBlocks, nil
}
