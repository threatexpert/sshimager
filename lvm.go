package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"sshimager/bitmap"
)

// LVInfo describes a logical volume on a PV partition
type LVInfo struct {
	Name        string // e.g. "cs-root"
	DevPath     string // e.g. "/dev/mapper/cs-root"
	StartSector uint64 // start sector on PV (from dmsetup table)
	SizeSectors uint64 // size in sectors
}

// LVMBuildBitmap builds a combined bitmap for an LVM PV partition.
// It discovers LVs via dmsetup table, opens each LV via SFTP to read
// its filesystem bitmap, then maps used blocks back to PV physical offsets.
func LVMBuildBitmap(conn *SSHConn, partOffset, partSize uint64, partDevPath string) (*bitmap.BlockBitmap, error) {
	// Get major:minor of the PV partition
	statOut, err := conn.ExecCommand(fmt.Sprintf("stat -c '%%t:%%T' %s 2>/dev/null", partDevPath))
	if err != nil {
		return nil, fmt.Errorf("cannot stat %s: %w", partDevPath, err)
	}
	var pvMaj, pvMin uint32
	fmt.Sscanf(strings.TrimSpace(statOut), "%x:%x", &pvMaj, &pvMin)
	if pvMaj == 0 && pvMin == 0 {
		return nil, fmt.Errorf("cannot get major:minor for %s", partDevPath)
	}

	// Get dmsetup table
	dmTable, err := conn.ExecCommandSudo("dmsetup table 2>/dev/null")
	if err != nil || dmTable == "" {
		return nil, fmt.Errorf("dmsetup table failed")
	}

	// Find LVs that map to this PV
	var lvs []LVInfo
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
		lvs = append(lvs, LVInfo{
			Name:        dmName,
			DevPath:     "/dev/mapper/" + dmName,
			StartSector: pvStart,
			SizeSectors: sizeSec,
		})
	}

	if len(lvs) == 0 {
		return nil, fmt.Errorf("no LVs found on PV %s", partDevPath)
	}

	// Create bitmap for the entire PV partition (512-byte sector granularity
	// but we'll use 4KB blocks for simplicity — matching typical fs block size)
	const blockSize = 4096
	totalBlocks := partSize / blockSize
	bitmapBytes := (totalBlocks + 7) / 8
	bm := &bitmap.BlockBitmap{
		Bits:        make([]byte, bitmapBytes),
		BlockSize:   blockSize,
		TotalBlocks: totalBlocks,
	}

	// Mark LVM metadata area (before first LV) as used
	// Typically first ~1MB is PV header + metadata
	metaBlocks := uint64(1024*1024) / blockSize // 1MB
	for b := uint64(0); b < metaBlocks && b < totalBlocks; b++ {
		bm.Bits[b/8] |= 1 << (b % 8)
	}

	// For each LV, read its filesystem bitmap and map back to PV
	for _, lv := range lvs {
		fmt.Fprintf(os.Stderr, "    LV %-20s %s (pvStart=%d sectors)\n",
			lv.Name, FormatSize(lv.SizeSectors*512), lv.StartSector)

		lvFile, err := conn.sftpClient.Open(lv.DevPath)
		if err != nil {
			// Cannot open LV — mark entire LV area as used
			fmt.Fprintf(os.Stderr, "    Warning: cannot open %s, marking as used\n", lv.DevPath)
			markLVUsed(bm, lv, blockSize, partOffset)
			continue
		}

		lvSize := lv.SizeSectors * 512
		lvBitmap, err := readLVBitmap(lvFile, lvSize)
		lvFile.Close()

		if err != nil {
			fmt.Fprintf(os.Stderr, "    Warning: bitmap read failed for %s, marking as used\n", lv.Name)
			markLVUsed(bm, lv, blockSize, partOffset)
			continue
		}

		// Map LV bitmap back to PV physical blocks
		mapLVBitmapToPV(bm, lvBitmap, lv, blockSize, partOffset)
	}

	return bm, nil
}

// readLVBitmap reads the bitmap of a filesystem inside an LV
func readLVBitmap(r io.ReaderAt, lvSize uint64) (*bitmap.BlockBitmap, error) {
	// Detect filesystem type
	buf := make([]byte, 4096)
	n, err := r.ReadAt(buf, 0)
	if err != nil && n < 512 {
		return nil, fmt.Errorf("readLVBitmap: ReadAt(0) failed: read %d bytes, err=%v", n, err)
	}

	// ext4
	if n >= 1082 {
		magic := uint16(buf[1024+56]) | uint16(buf[1024+57])<<8
		if magic == 0xEF53 {
			fmt.Fprintf(os.Stderr, "      LV fs: ext4\n")
			return bitmap.Ext4ReadBitmap(r, 0, lvSize)
		}
	}

	// XFS
	if n >= 4 && string(buf[0:4]) == "XFSB" {
		fmt.Fprintf(os.Stderr, "      LV fs: xfs (lvSize=%s)\n", FormatSize(lvSize))
		return bitmap.XFSReadBitmap(r, 0, lvSize)
	}

	// Swap
	swBuf := make([]byte, 10)
	if sn, _ := r.ReadAt(swBuf, 4086); sn == 10 {
		sig := string(swBuf)
		if sig == "SWAPSPACE2" || sig == "SWAP-SPACE" {
			fmt.Fprintf(os.Stderr, "      LV fs: swap (skipping, only header marked)\n")
			blockSize := uint32(4096)
			totalBlocks := lvSize / uint64(blockSize)
			bitmapBytes := (totalBlocks + 7) / 8
			bm := &bitmap.BlockBitmap{
				Bits:        make([]byte, bitmapBytes),
				BlockSize:   blockSize,
				TotalBlocks: totalBlocks,
			}
			if totalBlocks > 0 {
				bm.Bits[0] |= 1
			}
			return bm, nil
		}
	}

	// Dump first 16 bytes for diagnostics
	fmt.Fprintf(os.Stderr, "      LV fs: UNKNOWN (first 16 bytes: %02x)\n", buf[:16])
	return nil, fmt.Errorf("unknown filesystem in LV")
}

func markLVUsed(pvBitmap *bitmap.BlockBitmap, lv LVInfo, blockSize uint32, partOffset uint64) {
	// LV's physical start on PV (in bytes from partition start)
	lvPhysStart := lv.StartSector * 512
	lvPhysEnd := lvPhysStart + lv.SizeSectors*512

	startBlock := lvPhysStart / uint64(blockSize)
	endBlock := (lvPhysEnd + uint64(blockSize) - 1) / uint64(blockSize)
	if endBlock > pvBitmap.TotalBlocks {
		endBlock = pvBitmap.TotalBlocks
	}
	for b := startBlock; b < endBlock; b++ {
		pvBitmap.Bits[b/8] |= 1 << (b % 8)
	}
}

func mapLVBitmapToPV(pvBitmap *bitmap.BlockBitmap, lvBitmap *bitmap.BlockBitmap, lv LVInfo,
	pvBlockSize uint32, partOffset uint64) {

	lvBlockSize := uint64(lvBitmap.BlockSize)
	pvBlkSize := uint64(pvBlockSize)

	// LV physical start on PV (bytes from PV/partition start)
	lvPhysStart := lv.StartSector * 512

	for b := uint64(0); b < lvBitmap.TotalBlocks; b++ {
		if !lvBitmap.IsUsed(b) {
			continue
		}
		// This LV block is used. Map to PV physical offset.
		lvByteOff := b * lvBlockSize
		pvByteOff := lvPhysStart + lvByteOff

		// Mark corresponding PV blocks
		pvBlockStart := pvByteOff / pvBlkSize
		pvBlockEnd := (pvByteOff + lvBlockSize + pvBlkSize - 1) / pvBlkSize
		for pb := pvBlockStart; pb < pvBlockEnd && pb < pvBitmap.TotalBlocks; pb++ {
			pvBitmap.Bits[pb/8] |= 1 << (pb % 8)
		}
	}
}
