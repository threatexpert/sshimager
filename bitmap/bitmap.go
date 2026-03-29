// Package bitmap provides filesystem-aware block allocation bitmap readers.
// It supports ext4, XFS, FAT32, FAT16/FAT12, and NTFS.
// Both the sshimager client and the remote agent share this package
// to avoid duplicating filesystem parsing logic.
package bitmap

import (
	"encoding/binary"
	"fmt"
	"io"
)

// BlockBitmap represents a bitmap of used/free blocks for a partition.
// bit=1 means used, bit=0 means free.
type BlockBitmap struct {
	Bits        []byte
	BlockSize   uint32
	TotalBlocks uint64
}

// IsUsed returns true if the block at blockIdx is marked as used.
func (bm *BlockBitmap) IsUsed(blockIdx uint64) bool {
	byteIdx := blockIdx / 8
	bitIdx := blockIdx % 8
	if byteIdx >= uint64(len(bm.Bits)) {
		return true // out of range = assume used
	}
	return bm.Bits[byteIdx]&(1<<bitIdx) != 0
}

// EncodeMeta packs bitmap metadata for the agent wire protocol.
// Format: [4B blockSize] [8B totalBlocks] [bitmap bits...]
func EncodeMeta(bm *BlockBitmap) []byte {
	meta := make([]byte, 12+len(bm.Bits))
	binary.LittleEndian.PutUint32(meta[0:], bm.BlockSize)
	binary.LittleEndian.PutUint64(meta[4:], bm.TotalBlocks)
	copy(meta[12:], bm.Bits)
	return meta
}

// ReadFullAt reads exactly len(buf) bytes from r at offset.
func ReadFullAt(r io.ReaderAt, buf []byte, offset int64) error {
	n, err := r.ReadAt(buf, offset)
	if n == len(buf) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("short read: got %d, want %d", n, len(buf))
}
