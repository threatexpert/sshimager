package bitmap

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Fat16ReadBitmap reads the FAT12/FAT16 File Allocation Table to build a used-block bitmap.
// FAT type is determined by cluster count (Microsoft spec):
//
//	< 4085 clusters -> FAT12 (12-bit entries)
//	otherwise -> FAT16 (16-bit entries)
func Fat16ReadBitmap(r io.ReaderAt, partOffset, partSize uint64) (*BlockBitmap, error) {
	bs := make([]byte, 512)
	if err := ReadFullAt(r, bs, int64(partOffset)); err != nil {
		return nil, fmt.Errorf("fat16: cannot read boot sector: %w", err)
	}

	if bs[510] != 0x55 || bs[511] != 0xAA {
		return nil, fmt.Errorf("fat16: bad boot signature")
	}

	bytesPerSec := binary.LittleEndian.Uint16(bs[11:13])
	secPerClus := bs[13]
	rsvdSecCnt := binary.LittleEndian.Uint16(bs[14:16])
	numFATs := bs[16]
	rootEntCnt := binary.LittleEndian.Uint16(bs[17:19])
	fatSz16 := binary.LittleEndian.Uint16(bs[22:24])

	totSec := uint64(binary.LittleEndian.Uint16(bs[19:21]))
	if totSec == 0 {
		totSec = uint64(binary.LittleEndian.Uint32(bs[32:36]))
	}

	if bytesPerSec == 0 || secPerClus == 0 || rsvdSecCnt == 0 || numFATs == 0 || fatSz16 == 0 || totSec == 0 {
		return nil, fmt.Errorf("fat16: invalid BPB parameters")
	}

	clusterSize := uint32(bytesPerSec) * uint32(secPerClus)
	rootDirSectors := uint64((uint32(rootEntCnt)*32 + uint32(bytesPerSec) - 1) / uint32(bytesPerSec))
	fatStartSec := uint64(rsvdSecCnt)
	dataStartSec := fatStartSec + uint64(numFATs)*uint64(fatSz16) + rootDirSectors
	dataSectors := totSec - dataStartSec
	totalClusters := dataSectors / uint64(secPerClus)

	isFat12 := totalClusters < 4085

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

	dataStartBlock := dataStartSec * uint64(bytesPerSec) / uint64(clusterSize)
	for b := uint64(0); b < dataStartBlock && b < totalBlocks; b++ {
		bm.Bits[b/8] |= 1 << (b % 8)
	}

	fatOffset := partOffset + fatStartSec*uint64(bytesPerSec)
	fatSize := uint64(fatSz16) * uint64(bytesPerSec)
	fat := make([]byte, fatSize)
	if err := ReadFullAt(r, fat, int64(fatOffset)); err != nil {
		return nil, fmt.Errorf("fat16: cannot read FAT: %w", err)
	}

	for i := uint64(0); i < totalClusters; i++ {
		fatIdx := i + 2
		var entry uint64
		if isFat12 {
			entry = fat12Entry(fat, fatIdx)
		} else {
			off := fatIdx * 2
			if off+2 > uint64(len(fat)) {
				break
			}
			entry = uint64(binary.LittleEndian.Uint16(fat[off : off+2]))
		}
		if entry != 0 {
			block := dataStartBlock + i
			if block < totalBlocks {
				bm.Bits[block/8] |= 1 << (block % 8)
			}
		}
	}

	return bm, nil
}

// fat12Entry reads a 12-bit FAT entry at the given index.
func fat12Entry(fat []byte, idx uint64) uint64 {
	off := idx * 3 / 2
	if off+2 > uint64(len(fat)) {
		return 0xFFF // treat as used (end-of-chain)
	}
	val := uint16(fat[off]) | uint16(fat[off+1])<<8
	if idx%2 == 0 {
		return uint64(val & 0x0FFF)
	}
	return uint64(val >> 4)
}
