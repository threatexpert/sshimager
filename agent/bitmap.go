package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

// readFullAt reads exactly len(buf) bytes from r at offset.
func readFullAt(r io.ReaderAt, buf []byte, offset int64) error {
	n, err := r.ReadAt(buf, offset)
	if n == len(buf) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("short read: got %d, want %d", n, len(buf))
}

// ── ext4 bitmap ──

func readExt4Bitmap(r io.ReaderAt, partOffset, partSize uint64) ([]byte, uint32, uint64, error) {
	sb := make([]byte, 1024)
	if err := readFullAt(r, sb, int64(partOffset)+1024); err != nil {
		return nil, 0, 0, fmt.Errorf("ext4: cannot read superblock: %w", err)
	}

	magic := binary.LittleEndian.Uint16(sb[0x38:])
	if magic != 0xEF53 {
		return nil, 0, 0, fmt.Errorf("ext4: bad magic 0x%04X", magic)
	}

	rd32 := func(off int) uint32 { return binary.LittleEndian.Uint32(sb[off:]) }
	rd16 := func(off int) uint16 { return binary.LittleEndian.Uint16(sb[off:]) }

	blocksCountLo := rd32(0x04)
	logBlockSize := rd32(0x18)
	blocksPerGroup := rd32(0x20)
	incompat := rd32(0x60)

	blockSize := uint32(1024 << logBlockSize)

	var totalBlocks uint64
	if incompat&0x0002 != 0 {
		blocksCountHi := rd32(0x150)
		totalBlocks = uint64(blocksCountHi)<<32 | uint64(blocksCountLo)
	} else {
		totalBlocks = uint64(blocksCountLo)
	}

	numGroups := (totalBlocks + uint64(blocksPerGroup) - 1) / uint64(blocksPerGroup)

	descSize := uint32(32)
	if incompat&0x0080 != 0 {
		ds := rd16(0xFE)
		if ds > 32 {
			descSize = uint32(ds)
		}
	}

	var gdtOffset uint64
	if blockSize == 1024 {
		gdtOffset = partOffset + 2048
	} else {
		gdtOffset = partOffset + uint64(blockSize)
	}

	gdtSize := uint64(numGroups) * uint64(descSize)
	gdt := make([]byte, gdtSize)
	if err := readFullAt(r, gdt, int64(gdtOffset)); err != nil {
		return nil, 0, 0, fmt.Errorf("ext4: cannot read GDT: %w", err)
	}

	bitmapBytes := (totalBlocks + 7) / 8
	bits := make([]byte, bitmapBytes)

	inodesPerGroup := rd32(0x28)
	inodeSize := uint32(rd16(0x58))
	if inodeSize == 0 {
		inodeSize = 128
	}
	inodeTableBlocks := (inodesPerGroup * inodeSize + blockSize - 1) / blockSize

	roCompat := rd32(0x64)
	hasSparseSuper := roCompat&0x0001 != 0
	firstDataBlock := uint64(rd32(0x14))

	for bg := uint64(0); bg < numGroups; bg++ {
		descOff := bg * uint64(descSize)
		bbLo := binary.LittleEndian.Uint32(gdt[descOff+0x00:])
		var bbBlock uint64
		if descSize >= 64 {
			bbHi := binary.LittleEndian.Uint32(gdt[descOff+0x20:])
			bbBlock = uint64(bbHi)<<32 | uint64(bbLo)
		} else {
			bbBlock = uint64(bbLo)
		}

		startBit := firstDataBlock + bg*uint64(blocksPerGroup)

		if bbBlock == 0 || bbBlock >= totalBlocks {
			endBit := startBit + uint64(blocksPerGroup)
			if endBit > totalBlocks {
				endBit = totalBlocks
			}
			for b := startBit; b < endBit; b++ {
				bits[b/8] |= 1 << (b % 8)
			}
			continue
		}

		bmBuf := make([]byte, blockSize)
		bmOff := partOffset + bbBlock*uint64(blockSize)
		if err := readFullAt(r, bmBuf, int64(bmOff)); err != nil {
			return nil, 0, 0, fmt.Errorf("ext4: cannot read bitmap block %d (group %d): %w", bbBlock, bg, err)
		}

		bitsInGroup := uint64(blocksPerGroup)
		if startBit+bitsInGroup > totalBlocks {
			bitsInGroup = totalBlocks - startBit
		}

		startByte := startBit / 8
		srcBytes := (bitsInGroup + 7) / 8
		if startBit%8 == 0 && srcBytes <= uint64(blockSize) {
			copy(bits[startByte:startByte+srcBytes], bmBuf[:srcBytes])
		} else {
			for b := uint64(0); b < bitsInGroup; b++ {
				if bmBuf[b/8]&(1<<(b%8)) != 0 {
					bit := startBit + b
					bits[bit/8] |= 1 << (bit % 8)
				}
			}
		}
	}

	// Force-mark metadata blocks as used
	markUsed := func(block uint64) {
		if block < totalBlocks {
			bits[block/8] |= 1 << (block % 8)
		}
	}
	markUsedRange := func(start, count uint64) {
		for b := uint64(0); b < count; b++ {
			markUsed(start + b)
		}
	}

	for b := uint64(0); b <= firstDataBlock && b < totalBlocks; b++ {
		markUsed(b)
	}

	hasSBBackup := func(bg uint64) bool {
		if bg == 0 || !hasSparseSuper {
			return true
		}
		if bg == 1 {
			return true
		}
		for _, base := range []uint64{3, 5, 7} {
			n := base
			for n < bg {
				n *= base
			}
			if n == bg {
				return true
			}
		}
		return false
	}

	gdtBlocks := (uint64(numGroups)*uint64(descSize) + uint64(blockSize) - 1) / uint64(blockSize)

	for bg := uint64(0); bg < numGroups; bg++ {
		groupStart := bg * uint64(blocksPerGroup)

		if hasSBBackup(bg) {
			if blockSize == 1024 {
				if bg == 0 {
					markUsed(0)
					markUsed(1)
					markUsedRange(2, gdtBlocks)
				} else {
					markUsed(groupStart)
					markUsedRange(groupStart+1, gdtBlocks)
				}
			} else {
				markUsed(groupStart)
				markUsedRange(groupStart+1, gdtBlocks)
			}
		}

		descOff := bg * uint64(descSize)
		bbLo := binary.LittleEndian.Uint32(gdt[descOff+0x00:])
		ibLo := binary.LittleEndian.Uint32(gdt[descOff+0x04:])
		itLo := binary.LittleEndian.Uint32(gdt[descOff+0x08:])
		var bbBlk, ibBlk, itBlk uint64
		if descSize >= 64 {
			bbBlk = uint64(binary.LittleEndian.Uint32(gdt[descOff+0x20:]))<<32 | uint64(bbLo)
			ibBlk = uint64(binary.LittleEndian.Uint32(gdt[descOff+0x24:]))<<32 | uint64(ibLo)
			itBlk = uint64(binary.LittleEndian.Uint32(gdt[descOff+0x28:]))<<32 | uint64(itLo)
		} else {
			bbBlk = uint64(bbLo)
			ibBlk = uint64(ibLo)
			itBlk = uint64(itLo)
		}
		markUsed(bbBlk)
		markUsed(ibBlk)
		markUsedRange(itBlk, uint64(inodeTableBlocks))
	}

	return bits, blockSize, totalBlocks, nil
}

// ── XFS bitmap ──

func readXFSBitmap(r io.ReaderAt, partOffset, partSize uint64) ([]byte, uint32, uint64, error) {
	sb := make([]byte, 512)
	if err := readFullAt(r, sb, int64(partOffset)); err != nil {
		return nil, 0, 0, fmt.Errorf("xfs: cannot read superblock: %w", err)
	}
	if string(sb[0:4]) != "XFSB" {
		return nil, 0, 0, fmt.Errorf("xfs: bad magic")
	}

	blockSize := binary.BigEndian.Uint32(sb[4:8])
	totalBlocks := binary.BigEndian.Uint64(sb[8:16])
	agBlocks := binary.BigEndian.Uint32(sb[84:88])
	agCount := binary.BigEndian.Uint32(sb[88:92])
	sectSize := binary.BigEndian.Uint16(sb[102:104])
	if sectSize == 0 {
		sectSize = 512
	}

	bitmapBytes := (totalBlocks + 7) / 8
	bits := make([]byte, bitmapBytes)
	for i := range bits {
		bits[i] = 0xFF
	}
	if tail := totalBlocks % 8; tail > 0 {
		bits[len(bits)-1] = (1 << tail) - 1
	}

	for ag := uint32(0); ag < agCount; ag++ {
		agOffset := partOffset + uint64(ag)*uint64(agBlocks)*uint64(blockSize)
		agStartBlock := uint64(ag) * uint64(agBlocks)

		agf := make([]byte, sectSize)
		if err := readFullAt(r, agf, int64(agOffset)+int64(sectSize)); err != nil {
			return nil, 0, 0, fmt.Errorf("xfs: cannot read AGF for AG %d: %w", ag, err)
		}

		agfMagic := binary.BigEndian.Uint32(agf[0:4])
		if agfMagic != 0x58414746 {
			return nil, 0, 0, fmt.Errorf("xfs: bad AGF magic 0x%08X in AG %d", agfMagic, ag)
		}

		bnoRoot := binary.BigEndian.Uint32(agf[16:20])
		bnoLevel := binary.BigEndian.Uint32(agf[28:32])

		if err := xfsWalkBnobt(r, bits, totalBlocks, partOffset, agStartBlock,
			uint64(bnoRoot)*uint64(blockSize)+agOffset,
			int(bnoLevel), blockSize); err != nil {
			return nil, 0, 0, fmt.Errorf("xfs: bnobt walk failed in AG %d: %w", ag, err)
		}
	}

	return bits, blockSize, totalBlocks, nil
}

func xfsWalkBnobt(r io.ReaderAt, bits []byte, totalBlocks uint64,
	partOffset, agStartBlock, nodeOff uint64, level int, blockSize uint32) error {

	buf := make([]byte, blockSize)
	if err := readFullAt(r, buf, int64(nodeOff)); err != nil {
		return err
	}

	magic := binary.BigEndian.Uint32(buf[0:4])
	numRecs := binary.BigEndian.Uint16(buf[6:8])

	var hdrSize int
	switch magic {
	case 0x41423342: // "AB3B" v5
		hdrSize = 56
	default:
		hdrSize = 16
	}

	bbLevel := binary.BigEndian.Uint16(buf[4:6])
	if bbLevel > 0 {
		maxRecs := (int(blockSize) - hdrSize) / (8 + 4)
		ptrOffset := hdrSize + maxRecs*8
		for i := 0; i < int(numRecs); i++ {
			childAgBlock := binary.BigEndian.Uint32(buf[ptrOffset+i*4:])
			agOffset := partOffset + agStartBlock*uint64(blockSize)
			childOff := agOffset + uint64(childAgBlock)*uint64(blockSize)
			if err := xfsWalkBnobt(r, bits, totalBlocks, partOffset, agStartBlock, childOff, level-1, blockSize); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < int(numRecs); i++ {
			recOff := hdrSize + i*8
			if recOff+8 > len(buf) {
				break
			}
			freeStart := binary.BigEndian.Uint32(buf[recOff:])
			freeCount := binary.BigEndian.Uint32(buf[recOff+4:])
			absStart := agStartBlock + uint64(freeStart)
			for b := uint64(0); b < uint64(freeCount); b++ {
				bit := absStart + b
				if bit < totalBlocks {
					bits[bit/8] &^= 1 << (bit % 8)
				}
			}
		}
	}
	return nil
}
