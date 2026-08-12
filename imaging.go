package main

import (
	"context"
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
	RegionCopy RegionType = iota
	RegionUsedOnly
	RegionSkip
)

type Progress struct {
	TotalDone   uint64
	TotalWork   uint64 // estimated total bytes of real work (adjusted as bitmaps resolve)
	DataWritten uint64

	Context  context.Context
	Observer func(ImagingEvent)
	Stage    string

	lastEventTime    time.Time
	lastEventWritten uint64
	eventSpeed       float64
}

// ImagingEvent is a transport-neutral update suitable for a terminal, GUI,
// service, or test observer.
type ImagingEvent struct {
	Kind        string  `json:"kind"`
	Stage       string  `json:"stage,omitempty"`
	Message     string  `json:"message,omitempty"`
	Done        uint64  `json:"done,omitempty"`
	Total       uint64  `json:"total,omitempty"`
	DataWritten uint64  `json:"dataWritten,omitempty"`
	Percent     float64 `json:"percent,omitempty"`
	SpeedMBps   float64 `json:"speedMBps,omitempty"`
	ETASeconds  int64   `json:"etaSeconds,omitempty"`
}

func (p *Progress) err() error {
	if p.Context == nil {
		return nil
	}
	select {
	case <-p.Context.Done():
		return p.Context.Err()
	default:
		return nil
	}
}

func (p *Progress) report(tStart time.Time, force bool) {
	printProgress(p.TotalDone, p.TotalWork, p.DataWritten, tStart)
	if p.Observer == nil {
		return
	}

	now := time.Now()
	if !force && !p.lastEventTime.IsZero() && now.Sub(p.lastEventTime) < 250*time.Millisecond {
		return
	}
	dt := now.Sub(p.lastEventTime).Seconds()
	if p.lastEventTime.IsZero() {
		dt = now.Sub(tStart).Seconds()
	}
	if dt > 0 {
		delta := p.DataWritten - p.lastEventWritten
		current := float64(delta) / dt / 1048576
		if p.eventSpeed == 0 {
			p.eventSpeed = current
		} else {
			p.eventSpeed = p.eventSpeed*0.7 + current*0.3
		}
	}
	p.lastEventTime = now
	p.lastEventWritten = p.DataWritten

	percent := float64(0)
	if p.TotalWork > 0 {
		percent = float64(p.TotalDone) / float64(p.TotalWork) * 100
		if percent > 100 {
			percent = 100
		}
	}
	eta := int64(0)
	if p.eventSpeed > 0.1 && p.TotalDone < p.TotalWork {
		eta = int64(float64(p.TotalWork-p.TotalDone) / (p.eventSpeed * 1048576))
	}
	p.Observer(ImagingEvent{
		Kind:        "progress",
		Stage:       p.Stage,
		Done:        p.TotalDone,
		Total:       p.TotalWork,
		DataWritten: p.DataWritten,
		Percent:     percent,
		SpeedMBps:   p.eventSpeed,
		ETASeconds:  eta,
	})
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
	Backend  DiskBackend
	Disk     *DiskInfo
	Output   string
	Format   VDiskFormat
	BufSize  int
	Regions  []Region
	Context  context.Context
	Observer func(ImagingEvent)
}

const maxReconnectRetries = 9999

func emitImagingEvent(observer func(ImagingEvent), event ImagingEvent) {
	if observer != nil {
		observer(event)
	}
}

func reconnectWithRetry(ctx context.Context, backend DiskBackend, observer func(ImagingEvent)) error {
	delays := []time.Duration{1, 2, 5, 10, 30, 60}
	for attempt := 0; attempt < maxReconnectRetries; attempt++ {
		delay := delays[len(delays)-1]
		if attempt < len(delays) {
			delay = delays[attempt]
		}
		fmt.Fprintf(os.Stderr, "\nConnection lost. Retry %d in %ds...\n",
			attempt+1, int(delay))
		emitImagingEvent(observer, ImagingEvent{
			Kind: "reconnecting", Stage: "reconnecting",
			Message: fmt.Sprintf("连接中断，%d 秒后进行第 %d 次重连", int(delay), attempt+1),
		})
		timer := time.NewTimer(delay * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		if err := backend.Reconnect(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			fmt.Fprintf(os.Stderr, "Reconnect failed: %v\n", err)
			emitImagingEvent(observer, ImagingEvent{Kind: "log", Stage: "reconnecting", Message: fmt.Sprintf("重连失败：%v", err)})
			continue
		}
		fmt.Fprintf(os.Stderr, "Reconnected.\n")
		emitImagingEvent(observer, ImagingEvent{Kind: "log", Stage: "reconnecting", Message: "SSH 已重新连接，继续备份"})
		ResetProgress()
		return nil
	}
	return fmt.Errorf("reconnect failed after %d attempts", maxReconnectRetries)
}

func RunImaging(cfg *ImagingConfig) error {
	ctx := cfg.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	bufSize := cfg.BufSize
	if bufSize == 0 {
		bufSize = defaultBufSize
	}

	vw, err := CreateVDisk(cfg.Output, cfg.Format, cfg.Disk.Size)
	if err != nil {
		return fmt.Errorf("cannot create output image: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = vw.Close()
		}
	}()

	writeInfoFile(cfg)

	fmt.Fprintf(os.Stderr, "Creating %s image: %s\n", cfg.Format, cfg.Output)
	tStart := time.Now()
	buf := make([]byte, bufSize)
	prog := &Progress{TotalWork: cfg.Disk.Size, Context: ctx, Observer: cfg.Observer}

	for _, region := range cfg.Regions {
		if err := prog.err(); err != nil {
			return err
		}
		switch region.Type {
		case RegionSkip:
			pname := regionName(cfg.Disk, &region)
			eventName := regionEventName(cfg.Disk, &region)
			prog.Stage = "跳过 " + eventName
			emitImagingEvent(cfg.Observer, ImagingEvent{Kind: "stage", Stage: prog.Stage, Message: fmt.Sprintf("跳过 %s，大小 %s", eventName, FormatSize(region.Length))})
			fmt.Fprintf(os.Stderr, "  Partition %s: EXCLUDED — skipping %s\n", pname, FormatSize(region.Length))
			if err := vw.WriteZero(region.Offset, region.Length); err != nil {
				return err
			}
			prog.TotalWork -= region.Length

		case RegionCopy:
			pname := regionName(cfg.Disk, &region)
			eventName := regionEventName(cfg.Disk, &region)
			prog.Stage = "复制 " + eventName
			emitImagingEvent(cfg.Observer, ImagingEvent{Kind: "stage", Stage: prog.Stage, Message: fmt.Sprintf("开始复制 %s，大小 %s", eventName, FormatSize(region.Length))})
			fmt.Fprintf(os.Stderr, "  Copying %s: %s ...\n", pname, FormatSize(region.Length))
			if err := copyRegion(cfg.Backend, vw, region.Offset, region.Length, buf, prog, tStart); err != nil {
				return err
			}

		case RegionUsedOnly:
			if region.PartIdx < 0 {
				continue
			}
			p := &cfg.Disk.Partitions[region.PartIdx]
			prog.Stage = fmt.Sprintf("分区 #%d 仅复制已用块", p.Number)
			emitImagingEvent(cfg.Observer, ImagingEvent{Kind: "stage", Stage: prog.Stage, Message: fmt.Sprintf("正在分析分区 #%d %s 的 %s 文件系统位图", p.Number, p.DevPath, p.FSType)})
			fmt.Fprintf(os.Stderr, "  Partition #%d %s %s: used-only %s ...\n",
				p.Number, p.FSType, p.Mountpoint, FormatSize(region.Length))
			if err := copyUsedOnly(cfg.Backend, vw, p, buf, prog, tStart); err != nil {
				return err
			}
		}
	}

	if err := vw.Close(); err != nil {
		return fmt.Errorf("close image failed: %w", err)
	}
	closed = true

	elapsed := time.Since(tStart).Seconds()
	if elapsed < 0.1 {
		elapsed = 0.1
	}
	fmt.Fprintf(os.Stderr, "\nDone. %s transferred in %.1f seconds (%.0f MB/s)\n",
		FormatSize(prog.DataWritten), elapsed,
		float64(prog.DataWritten)/elapsed/1000000)

	os.Chmod(cfg.Output, 0444)
	fmt.Fprintf(os.Stderr, "Output set to read-only: %s\n", cfg.Output)
	prog.TotalDone = prog.TotalWork
	prog.Stage = "完成"
	prog.report(tStart, true)
	return nil
}

func regionName(disk *DiskInfo, region *Region) string {
	if region.PartIdx >= 0 && region.PartIdx < len(disk.Partitions) {
		p := &disk.Partitions[region.PartIdx]
		return fmt.Sprintf("#%d %s %s", p.Number, p.FSType, p.Mountpoint)
	}
	return "gap/tail"
}

// regionEventName produces a user-facing description for GUI task logs. Raw
// disk images also contain partition tables, alignment gaps, and trailing
// sectors, so not every copied region is a filesystem partition.
func regionEventName(disk *DiskInfo, region *Region) string {
	if region.PartIdx >= 0 && region.PartIdx < len(disk.Partitions) {
		p := &disk.Partitions[region.PartIdx]
		location := p.Mountpoint
		if location == "" {
			location = p.FSLabel
		}
		if location == "" {
			location = "未挂载"
		}
		return fmt.Sprintf("分区 #%d %s（%s，%s）", p.Number, p.DevPath, p.FSType, location)
	}
	if region.Offset == 0 {
		return "磁盘头部/分区表区域"
	}
	if region.Offset+region.Length >= disk.Size {
		return "磁盘尾部区域"
	}
	return fmt.Sprintf("分区间隙区域（起始偏移 %s）", FormatSize(region.Offset))
}

// StreamingBackend is optionally implemented by backends that support
// server-push streaming (agent mode). This avoids per-chunk round trips.
type StreamingBackend interface {
	StreamCopyRegion(vw VDiskWriter, offset, length uint64, chunkSize uint32,
		prog *Progress, tStart time.Time) error
}

// copyRegion copies a contiguous disk range with auto-reconnect on network errors.
func copyRegion(backend DiskBackend, vw VDiskWriter, offset, length uint64,
	buf []byte, prog *Progress, tStart time.Time) error {

	// Use streaming if the backend supports it
	if sb, ok := backend.(StreamingBackend); ok {
		return copyRegionStream(sb, backend, vw, offset, length, uint32(len(buf)), prog, tStart)
	}

	return copyRegionSerial(backend, vw, offset, length, buf, prog, tStart)
}

// copyRegionStream uses the streaming protocol with auto-reconnect.
// Any error while data remains triggers reconnect+retry — we don't care
// whether it's a "network" error; if imaging isn't done, just retry.
func copyRegionStream(sb StreamingBackend, backend DiskBackend, vw VDiskWriter,
	offset, length uint64, chunkSize uint32,
	prog *Progress, tStart time.Time) error {

	curOff := offset
	remaining := length

	for remaining > 0 {
		if err := prog.err(); err != nil {
			return err
		}
		savedTotalDone := prog.TotalDone

		err := sb.StreamCopyRegion(vw, curOff, remaining, chunkSize, prog, tStart)
		if err == nil {
			return nil
		}
		if cancelErr := prog.err(); cancelErr != nil {
			return cancelErr
		}

		// Calculate how far we got
		advanced := prog.TotalDone - savedTotalDone
		curOff += advanced
		remaining -= advanced

		if remaining == 0 {
			return nil
		}

		fmt.Fprintf(os.Stderr, "\nStream interrupted at offset %d (%s remaining): %v\n",
			curOff, FormatSize(remaining), err)
		if reconErr := reconnectWithRetry(prog.Context, backend, prog.Observer); reconErr != nil {
			return fmt.Errorf("reconnect failed: %w", reconErr)
		}
	}
	return nil
}

// copyRegionSerial is the original serial ReadAt loop (used by SFTP backend).
func copyRegionSerial(backend DiskBackend, vw VDiskWriter, offset, length uint64,
	buf []byte, prog *Progress, tStart time.Time) error {

	remaining := length
	curOff := offset

	for remaining > 0 {
		if err := prog.err(); err != nil {
			return err
		}
		toRead := remaining
		if toRead > uint64(len(buf)) {
			toRead = uint64(len(buf))
		}

		n, err := backend.ReadAt(buf[:toRead], int64(curOff))
		if err != nil {
			if n > 0 && err.Error() == "EOF" {
				// Normal EOF with valid data (e.g. last chunk of device) — use it
			} else if backend.IsNetworkError(err) {
				// Network error — data may be corrupt, discard and reconnect
				if reconErr := reconnectWithRetry(prog.Context, backend, prog.Observer); reconErr != nil {
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

		prog.report(tStart, false)
	}
	return nil
}

func copyUsedOnly(backend DiskBackend, vw VDiskWriter, part *PartitionInfo,
	buf []byte, prog *Progress, tStart time.Time) error {

	if err := prog.err(); err != nil {
		return err
	}

	// Swap: write zeros (sparse skip), no bitmap needed
	if part.FSType == FSSwap {
		fmt.Fprintf(os.Stderr, "    Swap: used-only — writing zeros (sparse skip)\n")
		if err := vw.WriteZero(part.Offset, part.Size); err != nil {
			return err
		}
		// Swap doesn't transfer data — shrink TotalWork
		prog.TotalWork -= part.Size
		return nil
	}

	// Get bitmap via backend (SFTP: client-side parse, Agent: server-side compute)
	bm, err := backend.GetBitmap(part.Offset, part.Size, part.FSType, part.DevPath)
	if err != nil {
		if cancelErr := prog.err(); cancelErr != nil {
			return cancelErr
		}
		return fmt.Errorf("bitmap read failed for partition #%d: %w", part.Number, err)
	}

	blockSize := uint64(bm.BlockSize)
	totalBlocks := bm.TotalBlocks

	usedBlocks := uint64(0)
	for b := uint64(0); b < totalBlocks; b++ {
		if b%65536 == 0 {
			if err := prog.err(); err != nil {
				return err
			}
		}
		if bm.IsUsed(b) {
			usedBlocks++
		}
	}
	usedBytes := usedBlocks * blockSize
	freeBytes := part.Size - usedBytes
	fmt.Fprintf(os.Stderr, "    Bitmap: %d/%d blocks used (%s / %s, block_size=%d)\n",
		usedBlocks, totalBlocks, FormatSize(usedBytes), FormatSize(part.Size), blockSize)

	// Adjust TotalWork: subtract free bytes that won't be transferred
	prog.TotalWork -= freeBytes

	runStart := uint64(0)
	inRun := false

	for b := uint64(0); b <= totalBlocks; b++ {
		if b%65536 == 0 {
			if err := prog.err(); err != nil {
				return err
			}
		}
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
			if err := copyRegion(backend, vw, off, runLen, buf, prog, tStart); err != nil {
				return err
			}
			inRun = false
		}
	}

	return nil
}

func writeInfoFile(cfg *ImagingConfig) {
	path := cfg.Output + ".info"
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()

	user, host := cfg.Backend.RemoteInfo()

	fmt.Fprintf(f, "# sshimager v%s imaging config\n", Version)
	fmt.Fprintf(f, "# Created: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(f, "remote=%s@%s\n", user, host)
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
