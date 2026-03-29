package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"sshimager/bitmap"
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
func readLVMBitmap(disk io.ReaderAt, partOffset, partSize uint64, partDevPath string) (*bitmap.BlockBitmap, error) {
	// Get major:minor of the PV partition
	statOut, err := execCmd("stat", "-c", "%t:%T", partDevPath)
	if err != nil {
		return nil, fmt.Errorf("cannot stat %s: %w", partDevPath, err)
	}
	var pvMaj, pvMin uint32
	fmt.Sscanf(strings.TrimSpace(statOut), "%x:%x", &pvMaj, &pvMin)
	if pvMaj == 0 && pvMin == 0 {
		return nil, fmt.Errorf("cannot get major:minor for %s", partDevPath)
	}

	// Get dmsetup table
	dmTable, err := execCmd("dmsetup", "table")
	if err != nil || dmTable == "" {
		return nil, fmt.Errorf("dmsetup table failed")
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
		return nil, fmt.Errorf("no LVs found on PV %s", partDevPath)
	}

	// Create bitmap for the PV partition
	const blockSize = 4096
	totalBlocks := partSize / blockSize
	bitmapBytes := (totalBlocks + 7) / 8
	bm := &bitmap.BlockBitmap{
		Bits:        make([]byte, bitmapBytes),
		BlockSize:   blockSize,
		TotalBlocks: totalBlocks,
	}

	// Mark LVM metadata area (first ~1MB)
	metaBlocks := uint64(1024*1024) / blockSize
	for b := uint64(0); b < metaBlocks && b < totalBlocks; b++ {
		bm.Bits[b/8] |= 1 << (b % 8)
	}

	// For each LV, open it locally and read its filesystem bitmap
	for _, lv := range lvs {
		lvFile, err := os.OpenFile(lv.DevPath, os.O_RDONLY, 0)
		if err != nil {
			// Cannot open — mark entire LV area as used
			markLVAreaUsed(bm, lv, blockSize)
			continue
		}

		lvSize := lv.SizeSectors * 512
		lvBm, err := readLVFilesystemBitmap(lvFile, lvSize)
		lvFile.Close()

		if err != nil {
			markLVAreaUsed(bm, lv, blockSize)
			continue
		}

		// Map LV bitmap back to PV physical blocks
		mapLVToPV(bm, lvBm, lv, blockSize)
	}

	return bm, nil
}

// readLVFilesystemBitmap detects the filesystem inside an LV and reads its bitmap
func readLVFilesystemBitmap(r io.ReaderAt, lvSize uint64) (*bitmap.BlockBitmap, error) {
	buf := make([]byte, 4096)
	n, err := r.ReadAt(buf, 0)
	if err != nil && n < 512 {
		return nil, fmt.Errorf("cannot read LV header")
	}

	// ext4
	if n >= 1082 {
		magic := uint16(buf[1024+56]) | uint16(buf[1024+57])<<8
		if magic == 0xEF53 {
			return bitmap.Ext4ReadBitmap(r, 0, lvSize)
		}
	}

	// XFS
	if n >= 4 && string(buf[0:4]) == "XFSB" {
		return bitmap.XFSReadBitmap(r, 0, lvSize)
	}

	// Swap — mark only first block
	swBuf := make([]byte, 10)
	if sn, _ := r.ReadAt(swBuf, 4086); sn == 10 {
		sig := string(swBuf)
		if sig == "SWAPSPACE2" || sig == "SWAP-SPACE" {
			bs := uint32(4096)
			total := lvSize / uint64(bs)
			bitmapBytes := (total + 7) / 8
			bm := &bitmap.BlockBitmap{
				Bits:        make([]byte, bitmapBytes),
				BlockSize:   bs,
				TotalBlocks: total,
			}
			if total > 0 {
				bm.Bits[0] = 1
			}
			return bm, nil
		}
	}

	return nil, fmt.Errorf("unknown filesystem in LV")
}

func markLVAreaUsed(bm *bitmap.BlockBitmap, lv lvInfo, blockSize uint32) {
	lvPhysStart := lv.StartSector * 512
	lvPhysEnd := lvPhysStart + lv.SizeSectors*512

	startBlock := lvPhysStart / uint64(blockSize)
	endBlock := (lvPhysEnd + uint64(blockSize) - 1) / uint64(blockSize)
	if endBlock > bm.TotalBlocks {
		endBlock = bm.TotalBlocks
	}
	for b := startBlock; b < endBlock; b++ {
		bm.Bits[b/8] |= 1 << (b % 8)
	}
}

func mapLVToPV(pvBm *bitmap.BlockBitmap, lvBm *bitmap.BlockBitmap, lv lvInfo, pvBlockSize uint32) {
	lvBlockSize := uint64(lvBm.BlockSize)
	pvBlkSize := uint64(pvBlockSize)
	lvPhysStart := lv.StartSector * 512

	for b := uint64(0); b < lvBm.TotalBlocks; b++ {
		if !lvBm.IsUsed(b) {
			continue
		}
		lvByteOff := b * lvBlockSize
		pvByteOff := lvPhysStart + lvByteOff

		pvBlockStart := pvByteOff / pvBlkSize
		pvBlockEnd := (pvByteOff + lvBlockSize + pvBlkSize - 1) / pvBlkSize
		for pb := pvBlockStart; pb < pvBlockEnd && pb < pvBm.TotalBlocks; pb++ {
			pvBm.Bits[pb/8] |= 1 << (pb % 8)
		}
	}
}
