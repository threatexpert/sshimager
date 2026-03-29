package bitmap

import (
	"encoding/binary"
	"fmt"
	"io"
)

// XFSReadBitmap reads the XFS block allocation bitmap by traversing
// the free-space-by-block B+tree (bnobt) in each Allocation Group.
// Bitmap starts all-1 (all used), then clears bits for free extents.
func XFSReadBitmap(r io.ReaderAt, partOffset, partSize uint64) (*BlockBitmap, error) {
	sb := make([]byte, 512)
	if err := ReadFullAt(r, sb, int64(partOffset)); err != nil {
		return nil, fmt.Errorf("xfs: cannot read superblock: %w", err)
	}

	if string(sb[0:4]) != "XFSB" {
		return nil, fmt.Errorf("xfs: bad magic")
	}

	// XFS superblock is big-endian
	blockSize := binary.BigEndian.Uint32(sb[4:8])
	totalBlocks := binary.BigEndian.Uint64(sb[8:16])
	agBlocks := binary.BigEndian.Uint32(sb[84:88])
	agCount := binary.BigEndian.Uint32(sb[88:92])
	sectSize := binary.BigEndian.Uint16(sb[102:104])
	if sectSize == 0 {
		sectSize = 512
	}

	// Allocate bitmap: all bits set to 1 (used)
	bitmapBytes := (totalBlocks + 7) / 8
	bm := &BlockBitmap{
		Bits:        make([]byte, bitmapBytes),
		BlockSize:   blockSize,
		TotalBlocks: totalBlocks,
	}
	for i := range bm.Bits {
		bm.Bits[i] = 0xFF
	}
	if tail := totalBlocks % 8; tail > 0 {
		bm.Bits[len(bm.Bits)-1] = (1 << tail) - 1
	}

	for ag := uint32(0); ag < agCount; ag++ {
		agOffset := partOffset + uint64(ag)*uint64(agBlocks)*uint64(blockSize)
		agStartBlock := uint64(ag) * uint64(agBlocks)

		agf := make([]byte, uint32(sectSize))
		if err := ReadFullAt(r, agf, int64(agOffset)+int64(sectSize)); err != nil {
			return nil, fmt.Errorf("xfs: cannot read AGF for AG %d: %w", ag, err)
		}

		agfMagic := binary.BigEndian.Uint32(agf[0:4])
		if agfMagic != 0x58414746 { // "XAGF"
			return nil, fmt.Errorf("xfs: bad AGF magic 0x%08X in AG %d", agfMagic, ag)
		}

		bnoRoot := binary.BigEndian.Uint32(agf[16:20])
		bnoLevel := binary.BigEndian.Uint32(agf[28:32])

		if err := xfsWalkBnobt(r, bm, partOffset, agStartBlock,
			uint64(bnoRoot)*uint64(blockSize)+agOffset,
			int(bnoLevel), blockSize); err != nil {
			return nil, fmt.Errorf("xfs: bnobt walk failed in AG %d: %w", ag, err)
		}
	}

	return bm, nil
}

func xfsWalkBnobt(r io.ReaderAt, bm *BlockBitmap, partOffset, agStartBlock uint64,
	nodeOff uint64, level int, blockSize uint32) error {

	buf := make([]byte, blockSize)
	if err := ReadFullAt(r, buf, int64(nodeOff)); err != nil {
		return err
	}

	magic := binary.BigEndian.Uint32(buf[0:4])
	bbLevel := binary.BigEndian.Uint16(buf[4:6])
	numRecs := binary.BigEndian.Uint16(buf[6:8])

	var hdrSize int
	switch magic {
	case 0x41423342: // "AB3B" — v5 short btree block with CRC
		hdrSize = 56
	case 0x41425442: // "ABTB" — v4 short btree block
		hdrSize = 16
	default:
		hdrSize = 16
	}

	if bbLevel > 0 {
		maxRecs := (int(blockSize) - hdrSize) / (8 + 4)
		ptrOffset := hdrSize + maxRecs*8

		for i := 0; i < int(numRecs); i++ {
			childAgBlock := binary.BigEndian.Uint32(buf[ptrOffset+i*4:])
			agOffset := partOffset + agStartBlock*uint64(blockSize)
			childOff := agOffset + uint64(childAgBlock)*uint64(blockSize)

			if err := xfsWalkBnobt(r, bm, partOffset, agStartBlock, childOff, level-1, blockSize); err != nil {
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
				if bit < bm.TotalBlocks {
					bm.Bits[bit/8] &^= 1 << (bit % 8)
				}
			}
		}
	}

	return nil
}
