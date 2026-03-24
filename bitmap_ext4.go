package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

// BlockBitmap represents a bitmap of used/free blocks for a partition.
// bit=1 means used, bit=0 means free.
type BlockBitmap struct {
	Bits       []byte
	BlockSize  uint32
	TotalBlocks uint64
}

func (bm *BlockBitmap) IsUsed(blockIdx uint64) bool {
	byteIdx := blockIdx / 8
	bitIdx := blockIdx % 8
	if byteIdx >= uint64(len(bm.Bits)) {
		return true // out of range = assume used
	}
	return bm.Bits[byteIdx]&(1<<bitIdx) != 0
}

// Ext4ReadBitmap reads the ext4 block allocation bitmap.
// r is the disk reader, partOffset is the absolute offset of the partition.
// Uses raw byte offsets into the superblock (no packed structs) to avoid
// alignment issues — same approach as the C version.
func Ext4ReadBitmap(r io.ReaderAt, partOffset, partSize uint64) (*BlockBitmap, error) {
	// Read superblock (1024 bytes at partition_offset + 1024)
	sb := make([]byte, 1024)
	if err := readAt(r, sb, int64(partOffset)+1024); err != nil {
		return nil, fmt.Errorf("ext4: cannot read superblock: %w", err)
	}

	// Verify magic
	magic := binary.LittleEndian.Uint16(sb[0x38:])
	if magic != 0xEF53 {
		return nil, fmt.Errorf("ext4: bad magic 0x%04X", magic)
	}

	// Read superblock fields by fixed offset (not struct — avoids padding bugs)
	rd32 := func(off int) uint32 { return binary.LittleEndian.Uint32(sb[off:]) }
	rd16 := func(off int) uint16 { return binary.LittleEndian.Uint16(sb[off:]) }

	blocksCountLo := rd32(0x04)
	logBlockSize := rd32(0x18)
	blocksPerGroup := rd32(0x20)
	incompat := rd32(0x60)

	blockSize := uint32(1024 << logBlockSize)

	var totalBlocks uint64
	if incompat&0x0002 != 0 { // INCOMPAT_META_BG or 64-bit
		blocksCountHi := rd32(0x150)
		totalBlocks = uint64(blocksCountHi)<<32 | uint64(blocksCountLo)
	} else {
		totalBlocks = uint64(blocksCountLo)
	}

	numGroups := (totalBlocks + uint64(blocksPerGroup) - 1) / uint64(blocksPerGroup)

	// Descriptor size: 32 for old, s_desc_size for 64-bit
	// s_desc_size is at offset 0xFE in superblock
	descSize := uint32(32)
	if incompat&0x0080 != 0 { // INCOMPAT_64BIT
		ds := rd16(0xFE)
		if ds > 32 {
			descSize = uint32(ds)
		}
	}

	// Read Group Descriptor Table
	// GDT starts at the block after superblock.
	// For block_size=1024: GDT at block 2 (byte offset 2048)
	// For block_size>=4096: GDT at block 1 (byte offset = block_size)
	var gdtOffset uint64
	if blockSize == 1024 {
		gdtOffset = partOffset + 2048
	} else {
		gdtOffset = partOffset + uint64(blockSize)
	}

	gdtSize := uint64(numGroups) * uint64(descSize)
	gdt := make([]byte, gdtSize)
	if err := readAt(r, gdt, int64(gdtOffset)); err != nil {
		return nil, fmt.Errorf("ext4: cannot read GDT: %w", err)
	}

	// Allocate bitmap
	bitmapBytes := (totalBlocks + 7) / 8
	bm := &BlockBitmap{
		Bits:        make([]byte, bitmapBytes),
		BlockSize:   blockSize,
		TotalBlocks: totalBlocks,
	}

	// Read superblock fields for metadata protection
	inodesPerGroup := rd32(0x28)
	inodeSize := uint32(rd16(0x58))
	if inodeSize == 0 {
		inodeSize = 128
	}
	inodeTableBlocks := (inodesPerGroup * inodeSize + blockSize - 1) / blockSize

	// Check sparse_super feature
	roCompat := rd32(0x64)
	hasSparseSuper := roCompat&0x0001 != 0

	// ext4 first_data_block: for block_size=1024 this is 1, for 4096+ this is 0.
	// Block numbering starts at first_data_block. Block group N manages
	// blocks [first_data_block + N*blocksPerGroup, first_data_block + (N+1)*blocksPerGroup).
	// The bitmap bit 0 in group N corresponds to block first_data_block + N*blocksPerGroup.
	firstDataBlock := uint64(rd32(0x14))

	// For each block group, read its block bitmap
	for bg := uint64(0); bg < numGroups; bg++ {
		descOff := bg * uint64(descSize)

		// Block bitmap block number (low 32 + high 32 if 64-bit)
		bbLo := binary.LittleEndian.Uint32(gdt[descOff+0x00:])
		var bbBlock uint64
		if descSize >= 64 {
			bbHi := binary.LittleEndian.Uint32(gdt[descOff+0x20:])
			bbBlock = uint64(bbHi)<<32 | uint64(bbLo)
		} else {
			bbBlock = uint64(bbLo)
		}

		// startBit: the absolute block number that bitmap bit 0 of this group maps to
		startBit := firstDataBlock + bg*uint64(blocksPerGroup)

		if bbBlock == 0 || bbBlock >= totalBlocks {
			// Invalid bitmap block — treat entire group as used
			endBit := startBit + uint64(blocksPerGroup)
			if endBit > totalBlocks {
				endBit = totalBlocks
			}
			for b := startBit; b < endBit; b++ {
				bm.Bits[b/8] |= 1 << (b % 8)
			}
			continue
		}

		// Read one block of bitmap data
		bmBuf := make([]byte, blockSize)
		bmOff := partOffset + bbBlock*uint64(blockSize)
		if err := readAt(r, bmBuf, int64(bmOff)); err != nil {
			return nil, fmt.Errorf("ext4: cannot read bitmap block %d (group %d): %w", bbBlock, bg, err)
		}

		// Copy bits into our bitmap
		bitsInGroup := uint64(blocksPerGroup)
		if startBit+bitsInGroup > totalBlocks {
			bitsInGroup = totalBlocks - startBit
		}

		// Fast path: copy whole bytes when startBit is byte-aligned
		startByte := startBit / 8
		srcBytes := (bitsInGroup + 7) / 8
		if startBit%8 == 0 && srcBytes <= uint64(blockSize) {
			copy(bm.Bits[startByte:startByte+srcBytes], bmBuf[:srcBytes])
		} else {
			// Slow path: bit-by-bit
			for b := uint64(0); b < bitsInGroup; b++ {
				if bmBuf[b/8]&(1<<(b%8)) != 0 {
					bit := startBit + b
					bm.Bits[bit/8] |= 1 << (bit % 8)
				}
			}
		}
	}

	// Force-mark all ext4 metadata blocks as used.
	// The block bitmap only tracks data block allocation. The following
	// structures may appear "free" in the bitmap but are essential:
	//   - Superblock + GDT (and backups in sparse_super groups)
	//   - Block bitmap block, inode bitmap block, inode table blocks
	//
	// Without this, used-only images of older ext4/ext3 filesystems
	// (e.g. RHEL 6) may not boot because superblock backups or inode
	// tables are missing.

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

	// Force-mark block 0 and all blocks before first_data_block as used.
	for b := uint64(0); b <= firstDataBlock && b < totalBlocks; b++ {
		markUsed(b)
	}

	// Check if a block group has superblock backup (sparse_super feature)
	hasSBBackup := func(bg uint64) bool {
		if bg == 0 {
			return true
		}
		if !hasSparseSuper {
			return true // all groups have backup if no sparse_super
		}
		// Backup in groups that are 0, 1, or powers of 3, 5, 7
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

	// GDT size in blocks
	gdtBlocks := (uint64(numGroups)*uint64(descSize) + uint64(blockSize) - 1) / uint64(blockSize)

	for bg := uint64(0); bg < numGroups; bg++ {
		groupStart := bg * uint64(blocksPerGroup)

		// 1. Superblock + GDT backup
		if hasSBBackup(bg) {
			if blockSize == 1024 {
				// block 0 = boot sector (before superblock), block 1 = superblock
				if bg == 0 {
					markUsed(0) // boot block
					markUsed(1) // superblock
					markUsedRange(2, gdtBlocks) // GDT
				} else {
					markUsed(groupStart)     // superblock backup
					markUsedRange(groupStart+1, gdtBlocks)
				}
			} else {
				// block 0 of group = superblock (for bg=0, this is block 0 which has
				// boot sector + superblock in the same block)
				markUsed(groupStart)
				markUsedRange(groupStart+1, gdtBlocks)
			}
		}

		// 2. Re-read this group's descriptor for bitmap/inode table blocks
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

		// 3. Block bitmap block
		markUsed(bbBlk)

		// 4. Inode bitmap block
		markUsed(ibBlk)

		// 5. Inode table blocks
		markUsedRange(itBlk, uint64(inodeTableBlocks))
	}

	return bm, nil
}
