package bitmap

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Ext4ReadBitmap reads the ext4 block allocation bitmap.
func Ext4ReadBitmap(r io.ReaderAt, partOffset, partSize uint64) (*BlockBitmap, error) {
	sb := make([]byte, 1024)
	if err := ReadFullAt(r, sb, int64(partOffset)+1024); err != nil {
		return nil, fmt.Errorf("ext4: cannot read superblock: %w", err)
	}

	magic := binary.LittleEndian.Uint16(sb[0x38:])
	if magic != 0xEF53 {
		return nil, fmt.Errorf("ext4: bad magic 0x%04X", magic)
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
	if incompat&0x0080 != 0 { // INCOMPAT_64BIT
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
	if err := ReadFullAt(r, gdt, int64(gdtOffset)); err != nil {
		return nil, fmt.Errorf("ext4: cannot read GDT: %w", err)
	}

	bitmapBytes := (totalBlocks + 7) / 8
	bm := &BlockBitmap{
		Bits:        make([]byte, bitmapBytes),
		BlockSize:   blockSize,
		TotalBlocks: totalBlocks,
	}

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
				bm.Bits[b/8] |= 1 << (b % 8)
			}
			continue
		}

		bmBuf := make([]byte, blockSize)
		bmOff := partOffset + bbBlock*uint64(blockSize)
		if err := ReadFullAt(r, bmBuf, int64(bmOff)); err != nil {
			return nil, fmt.Errorf("ext4: cannot read bitmap block %d (group %d): %w", bbBlock, bg, err)
		}

		bitsInGroup := uint64(blocksPerGroup)
		if startBit+bitsInGroup > totalBlocks {
			bitsInGroup = totalBlocks - startBit
		}

		startByte := startBit / 8
		srcBytes := (bitsInGroup + 7) / 8
		if startBit%8 == 0 && srcBytes <= uint64(blockSize) {
			copy(bm.Bits[startByte:startByte+srcBytes], bmBuf[:srcBytes])
		} else {
			for b := uint64(0); b < bitsInGroup; b++ {
				if bmBuf[b/8]&(1<<(b%8)) != 0 {
					bit := startBit + b
					bm.Bits[bit/8] |= 1 << (bit % 8)
				}
			}
		}
	}

	// Force-mark metadata blocks as used
	markUsed := func(block uint64) {
		if block < totalBlocks {
			bm.Bits[block/8] |= 1 << (block % 8)
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

	return bm, nil
}
