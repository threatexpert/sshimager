package main

import (
	"fmt"
	"os"
	"time"
)

type Region struct {
	Offset  uint64
	Length  uint64
	Type    RegionType
	PartIdx int
}

type RegionType int

const (
	RegionCopy     RegionType = iota
	RegionUsedOnly
	RegionSkip
)

type Progress struct {
	TotalDone   uint64
	DataWritten uint64
}

func BuildRegions(disk *DiskInfo) []Region {
	var regions []Region
	diskPos := uint64(0)

	for i := range disk.Partitions {
		p := &disk.Partitions[i]
		if diskPos < p.Offset {
			regions = append(regions, Region{
				Offset: diskPos, Length: p.Offset - diskPos,
				Type: RegionCopy, PartIdx: -1,
			})
		}
		switch p.CopyMode {
		case CopySkip:
			regions = append(regions, Region{
				Offset: p.Offset, Length: p.Size,
				Type: RegionSkip, PartIdx: i,
			})
		case CopyUsedOnly:
			regions = append(regions, Region{
				Offset: p.Offset, Length: p.Size,
				Type: RegionUsedOnly, PartIdx: i,
			})
		default:
			regions = append(regions, Region{
				Offset: p.Offset, Length: p.Size,
				Type: RegionCopy, PartIdx: i,
			})
		}
		diskPos = p.Offset + p.Size
	}

	if diskPos < disk.Size {
		regions = append(regions, Region{
			Offset: diskPos, Length: disk.Size - diskPos,
			Type: RegionCopy, PartIdx: -1,
		})
	}
	return regions
}

const defaultBufSize = 8 * 1024 * 1024

type ImagingConfig struct {
	Conn    *SSHConn
	Disk    *DiskInfo
	Output  string
	Format  VDiskFormat
	BufSize int
	Regions []Region
}

const maxReconnectRetries = 9999

func reconnectWithRetry(conn *SSHConn) error {
	delays := []time.Duration{1, 2, 5, 10, 30, 60}
	for attempt := 0; attempt < maxReconnectRetries; attempt++ {
		delay := delays[len(delays)-1]
		if attempt < len(delays) {
			delay = delays[attempt]
		}
		fmt.Fprintf(os.Stderr, "\nConnection lost. Retry %d in %ds...\n",
			attempt+1, int(delay))
		time.Sleep(delay * time.Second)

		if err := conn.Reconnect(); err != nil {
			fmt.Fprintf(os.Stderr, "Reconnect failed: %v\n", err)
			continue
		}
		fmt.Fprintf(os.Stderr, "Reconnected.\n")
		return nil
	}
	return fmt.Errorf("reconnect failed after %d attempts", maxReconnectRetries)
}

func RunImaging(cfg *ImagingConfig) error {
	bufSize := cfg.BufSize
	if bufSize == 0 {
		bufSize = defaultBufSize
	}

	vw, err := CreateVDisk(cfg.Output, cfg.Format, cfg.Disk.Size)
	if err != nil {
		return fmt.Errorf("cannot create output image: %w", err)
	}

	writeInfoFile(cfg)

	fmt.Fprintf(os.Stderr, "Creating %s image: %s\n", cfg.Format, cfg.Output)
	tStart := time.Now()
	buf := make([]byte, bufSize)
	prog := &Progress{}

	for _, region := range cfg.Regions {
		switch region.Type {
		case RegionSkip:
			pname := regionName(cfg.Disk, &region)
			fmt.Fprintf(os.Stderr, "  Partition %s: EXCLUDED — skipping %s\n", pname, FormatSize(region.Length))
			if err := vw.WriteZero(region.Offset, region.Length); err != nil {
				return err
			}
			prog.TotalDone += region.Length

		case RegionCopy:
			pname := regionName(cfg.Disk, &region)
			fmt.Fprintf(os.Stderr, "  Copying %s: %s ...\n", pname, FormatSize(region.Length))
			if err := copyRegion(cfg.Conn, vw, region.Offset, region.Length, buf, prog,
				cfg.Disk.Size, tStart); err != nil {
				return err
			}

		case RegionUsedOnly:
			if region.PartIdx < 0 {
				continue
			}
			p := &cfg.Disk.Partitions[region.PartIdx]
			fmt.Fprintf(os.Stderr, "  Partition #%d %s %s: used-only %s ...\n",
				p.Number, p.FSType, p.Mountpoint, FormatSize(region.Length))
			if err := copyUsedOnly(cfg.Conn, vw, p, buf, prog,
				cfg.Disk.Size, tStart); err != nil {
				return err
			}
		}
	}

	if err := vw.Close(); err != nil {
		return fmt.Errorf("close image failed: %w", err)
	}

	elapsed := time.Since(tStart).Seconds()
	if elapsed < 0.1 {
		elapsed = 0.1
	}
	fmt.Fprintf(os.Stderr, "\nDone. %s transferred in %.1f seconds (%.0f MB/s)\n",
		FormatSize(prog.DataWritten), elapsed,
		float64(prog.DataWritten)/elapsed/1000000)

	os.Chmod(cfg.Output, 0444)
	fmt.Fprintf(os.Stderr, "Output set to read-only: %s\n", cfg.Output)
	return nil
}

func regionName(disk *DiskInfo, region *Region) string {
	if region.PartIdx >= 0 && region.PartIdx < len(disk.Partitions) {
		p := &disk.Partitions[region.PartIdx]
		return fmt.Sprintf("#%d %s %s", p.Number, p.FSType, p.Mountpoint)
	}
	return "gap/tail"
}

// copyRegion copies a contiguous disk range with auto-reconnect on network errors.
func copyRegion(conn *SSHConn, vw VDiskWriter, offset, length uint64,
	buf []byte, prog *Progress, diskSize uint64, tStart time.Time) error {

	remaining := length
	curOff := offset

	for remaining > 0 {
		toRead := remaining
		if toRead > uint64(len(buf)) {
			toRead = uint64(len(buf))
		}

		n, err := conn.ReadAt(buf[:toRead], int64(curOff))
		if err != nil {
			if n > 0 && err.Error() == "EOF" {
				// Normal EOF with valid data (e.g. last chunk of device) — use it
			} else if IsNetworkError(err) {
				// Network error — data may be corrupt, discard and reconnect
				if reconErr := reconnectWithRetry(conn); reconErr != nil {
					return fmt.Errorf("connection lost, reconnect failed: %w", reconErr)
				}
				continue // retry from same offset
			} else {
				return fmt.Errorf("read error at offset %d: %w", curOff, err)
			}
		}
		if n == 0 {
			return fmt.Errorf("read returned 0 bytes at offset %d", curOff)
		}

		if err := vw.Write(curOff, buf[:n]); err != nil {
			return fmt.Errorf("write error at offset %d: %w", curOff, err)
		}

		curOff += uint64(n)
		remaining -= uint64(n)
		prog.TotalDone += uint64(n)
		prog.DataWritten += uint64(n)

		printProgress(prog.TotalDone, diskSize, prog.DataWritten, tStart)
	}
	return nil
}

func copyUsedOnly(conn *SSHConn, vw VDiskWriter, part *PartitionInfo,
	buf []byte, prog *Progress, diskSize uint64, tStart time.Time) error {

	var bm *BlockBitmap
	var err error

	switch part.FSType {
	case FSExt2, FSExt3, FSExt4:
		bm, err = Ext4ReadBitmap(conn, part.Offset, part.Size)
	case FSXFS:
		bm, err = XFSReadBitmap(conn, part.Offset, part.Size)
	case FSLVM:
		bm, err = LVMBuildBitmap(conn, part.Offset, part.Size, part.DevPath)
	case FSSwap:
		fmt.Fprintf(os.Stderr, "    Swap: used-only — writing zeros (sparse skip)\n")
		if err := vw.WriteZero(part.Offset, part.Size); err != nil {
			return err
		}
		prog.TotalDone += part.Size
		return nil
	default:
		return copyRegion(conn, vw, part.Offset, part.Size, buf, prog, diskSize, tStart)
	}

	if err != nil {
		return fmt.Errorf("bitmap read failed for partition #%d: %w", part.Number, err)
	}

	blockSize := uint64(bm.BlockSize)
	totalBlocks := bm.TotalBlocks

	usedBlocks := uint64(0)
	for b := uint64(0); b < totalBlocks; b++ {
		if bm.IsUsed(b) {
			usedBlocks++
		}
	}
	usedBytes := usedBlocks * blockSize
	fmt.Fprintf(os.Stderr, "    Bitmap: %d/%d blocks used (%s / %s, block_size=%d)\n",
		usedBlocks, totalBlocks, FormatSize(usedBytes), FormatSize(part.Size), blockSize)

	runStart := uint64(0)
	inRun := false

	for b := uint64(0); b <= totalBlocks; b++ {
		used := b < totalBlocks && bm.IsUsed(b)
		if used && !inRun {
			runStart = b
			inRun = true
		} else if !used && inRun {
			off := part.Offset + runStart*blockSize
			runLen := (b - runStart) * blockSize
			if off+runLen > part.Offset+part.Size {
				runLen = part.Offset + part.Size - off
			}
			if err := copyRegion(conn, vw, off, runLen, buf, prog, diskSize, tStart); err != nil {
				return err
			}
			inRun = false
		}
	}

	freeBytes := part.Size - usedBytes
	prog.TotalDone += freeBytes
	return nil
}

func writeInfoFile(cfg *ImagingConfig) {
	path := cfg.Output + ".info"
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()

	fmt.Fprintf(f, "# sshimager v%s imaging config\n", Version)
	fmt.Fprintf(f, "# Created: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(f, "remote=%s@%s\n", cfg.Conn.User, cfg.Conn.Host)
	fmt.Fprintf(f, "source_disk=%s\n", cfg.Disk.DevPath)
	fmt.Fprintf(f, "source_size=%d\n", cfg.Disk.Size)
	fmt.Fprintf(f, "output_file=%s\n", cfg.Output)
	fmt.Fprintf(f, "output_format=%s\n\n", cfg.Format)

	for _, p := range cfg.Disk.Partitions {
		fmt.Fprintf(f, "[partition.%d]\n", p.Number)
		fmt.Fprintf(f, "device=%s\noffset=%d\nsize=%d\nfilesystem=%s\n",
			p.DevPath, p.Offset, p.Size, p.FSType)
		fmt.Fprintf(f, "mountpoint=%s\ncopy_mode=%d\n\n", p.Mountpoint, p.CopyMode)
	}
}
