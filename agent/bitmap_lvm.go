package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

// lvInfo describes a logical volume on a PV partition
type lvInfo struct {
	Name        string
	DevPath     string // /dev/mapper/xxx
	StartSector uint64 // start sector on PV
	SizeSectors uint64
}

// readLVMBitmap builds a combined bitmap for an LVM PV partition.
// Since the agent runs on the target host, all operations are local.
func readLVMBitmap(disk io.ReaderAt, partOffset, partSize uint64, partDevPath string) ([]byte, uint32, uint64, error) {
	// Get major:minor of the PV partition
	statOut, err := execCmd("stat", "-c", "%t:%T", partDevPath)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("cannot stat %s: %w", partDevPath, err)
	}
	var pvMaj, pvMin uint32
	fmt.Sscanf(strings.TrimSpace(statOut), "%x:%x", &pvMaj, &pvMin)
	if pvMaj == 0 && pvMin == 0 {
		return nil, 0, 0, fmt.Errorf("cannot get major:minor for %s", partDevPath)
	}

	// Get dmsetup table
	dmTable, err := execCmd("dmsetup", "table")
	if err != nil || dmTable == "" {
		return nil, 0, 0, fmt.Errorf("dmsetup table failed")
	}

	// Find LVs that map to this PV
	var lvs []lvInfo
	for _, line := range strings.Split(dmTable, "\n") {
		colon := strings.Index(line, ":")
		if colon < 0 {
			continue
		}
		dmName := strings.TrimSpace(line[:colon])
		rest := strings.TrimSpace(line[colon+1:])

		var startSec, sizeSec uint64
		var mtype string
		var dMaj, dMin uint32
		var pvStart uint64
		n, _ := fmt.Sscanf(rest, "%d %d %s %d:%d %d", &startSec, &sizeSec, &mtype, &dMaj, &dMin, &pvStart)
		if n < 6 || mtype != "linear" {
			continue
		}
		if dMaj != pvMaj || dMin != pvMin {
			continue
		}
		lvs = append(lvs, lvInfo{
			Name:        dmName,
			DevPath:     "/dev/mapper/" + dmName,
			StartSector: pvStart,
			SizeSectors: sizeSec,
		})
	}

	if len(lvs) == 0 {
		return nil, 0, 0, fmt.Errorf("no LVs found on PV %s", partDevPath)
	}

	// Create bitmap for the PV partition
	const blockSize = 4096
	totalBlocks := partSize / blockSize
	bitmapBytes := (totalBlocks + 7) / 8
	bits := make([]byte, bitmapBytes)

	markUsed := func(block uint64) {
		if block < totalBlocks {
			bits[block/8] |= 1 << (block % 8)
		}
	}

	// Mark LVM metadata area (first ~1MB)
	metaBlocks := uint64(1024*1024) / blockSize
	for b := uint64(0); b < metaBlocks && b < totalBlocks; b++ {
		markUsed(b)
	}

	// For each LV, open it locally and read its filesystem bitmap
	for _, lv := range lvs {
		lvFile, err := os.OpenFile(lv.DevPath, os.O_RDONLY, 0)
		if err != nil {
			// Cannot open — mark entire LV area as used
			markLVAreaUsed(bits, lv, blockSize, totalBlocks)
			continue
		}

		lvSize := lv.SizeSectors * 512
		lvBits, lvBlockSize, lvTotalBlocks, err := readLVFilesystemBitmap(lvFile, lvSize)
		lvFile.Close()

		if err != nil {
			markLVAreaUsed(bits, lv, blockSize, totalBlocks)
			continue
		}

		// Map LV bitmap back to PV physical blocks
		mapLVToPV(bits, lvBits, lvBlockSize, lvTotalBlocks, lv, blockSize, totalBlocks)
	}

	return bits, blockSize, totalBlocks, nil
}

// readLVFilesystemBitmap detects the filesystem inside an LV and reads its bitmap
func readLVFilesystemBitmap(r io.ReaderAt, lvSize uint64) ([]byte, uint32, uint64, error) {
	buf := make([]byte, 4096)
	n, err := r.ReadAt(buf, 0)
	if err != nil && n < 512 {
		return nil, 0, 0, fmt.Errorf("cannot read LV header")
	}

	// ext4
	if n >= 1082 {
		magic := uint16(buf[1024+56]) | uint16(buf[1024+57])<<8
		if magic == 0xEF53 {
			return readExt4Bitmap(r, 0, lvSize)
		}
	}

	// XFS
	if n >= 4 && string(buf[0:4]) == "XFSB" {
		return readXFSBitmap(r, 0, lvSize)
	}

	// Swap — mark only first block
	swBuf := make([]byte, 10)
	if sn, _ := r.ReadAt(swBuf, 4086); sn == 10 {
		sig := string(swBuf)
		if sig == "SWAPSPACE2" || sig == "SWAP-SPACE" {
			blockSize := uint32(4096)
			total := lvSize / uint64(blockSize)
			bitmapBytes := (total + 7) / 8
			b := make([]byte, bitmapBytes)
			if len(b) > 0 {
				b[0] = 1
			}
			return b, blockSize, total, nil
		}
	}

	return nil, 0, 0, fmt.Errorf("unknown filesystem in LV")
}

func markLVAreaUsed(bits []byte, lv lvInfo, blockSize uint32, totalBlocks uint64) {
	lvPhysStart := lv.StartSector * 512
	lvPhysEnd := lvPhysStart + lv.SizeSectors*512

	startBlock := lvPhysStart / uint64(blockSize)
	endBlock := (lvPhysEnd + uint64(blockSize) - 1) / uint64(blockSize)
	if endBlock > totalBlocks {
		endBlock = totalBlocks
	}
	for b := startBlock; b < endBlock; b++ {
		bits[b/8] |= 1 << (b % 8)
	}
}

func mapLVToPV(pvBits []byte, lvBits []byte, lvBlockSize uint32, lvTotalBlocks uint64,
	lv lvInfo, pvBlockSize uint32, pvTotalBlocks uint64) {

	lvPhysStart := lv.StartSector * 512

	for b := uint64(0); b < lvTotalBlocks; b++ {
		// Check if this LV block is used
		byteIdx := b / 8
		bitIdx := b % 8
		if byteIdx >= uint64(len(lvBits)) || lvBits[byteIdx]&(1<<bitIdx) == 0 {
			continue
		}

		// Map to PV physical offset
		lvByteOff := b * uint64(lvBlockSize)
		pvByteOff := lvPhysStart + lvByteOff

		// Mark corresponding PV blocks
		pvBlockStart := pvByteOff / uint64(pvBlockSize)
		pvBlockEnd := (pvByteOff + uint64(lvBlockSize) + uint64(pvBlockSize) - 1) / uint64(pvBlockSize)
		for pb := pvBlockStart; pb < pvBlockEnd && pb < pvTotalBlocks; pb++ {
			pvBits[pb/8] |= 1 << (pb % 8)
		}
	}
}

// encodeBitmapMeta packs bitmap metadata for the protocol response
func encodeBitmapMeta(bits []byte, blockSize uint32, totalBlocks uint64) []byte {
	meta := make([]byte, 12+len(bits))
	binary.LittleEndian.PutUint32(meta[0:], blockSize)
	binary.LittleEndian.PutUint64(meta[4:], totalBlocks)
	copy(meta[12:], bits)
	return meta
}
