package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"

	"sshimager/bitmap"
	"sshimager/protocol"
)

const idleTimeout = 60 * time.Second

// lastActivity stores UnixNano of last received command (including ping).
// The watchdog goroutine checks this and exits the process if stale.
var lastActivity atomic.Int64

func touchActivity() {
	lastActivity.Store(time.Now().UnixNano())
}

func startWatchdog() {
	touchActivity()
	go func() {
		for {
			time.Sleep(10 * time.Second)
			last := time.Unix(0, lastActivity.Load())
			if time.Since(last) > idleTimeout {
				os.Exit(0)
			}
		}
	}()
}

func main() {
	serve := flag.Bool("serve", false, "Run in serve mode (stdin/stdout protocol)")
	selfDelete := flag.Bool("delete", false, "Delete own binary on startup")
	flag.Parse()

	if *selfDelete {
		os.Remove(os.Args[0])
	}

	if !*serve {
		fmt.Fprintf(os.Stderr, "sshimager-agent: use --serve to start protocol mode\n")
		os.Exit(1)
	}

	// Redirect stderr to /dev/null so sudo prompts and debug output
	// don't corrupt the binary protocol on stdout
	redirectStderr()

	if err := runServe(os.Stdin, os.Stdout); err != nil {
		os.Exit(1)
	}
}

func runServe(in io.Reader, out io.Writer) error {
	reader := bufio.NewReaderSize(in, 256*1024)
	writer := bufio.NewWriterSize(out, 256*1024)

	// Handshake: send our magic+version
	hs := &protocol.Handshake{Magic: protocol.Magic, Version: protocol.Version}
	if err := protocol.WriteHandshake(writer, hs); err != nil {
		return err
	}
	writer.Flush()

	// Read client handshake
	clientHS, err := protocol.ReadHandshake(reader)
	if err != nil {
		return fmt.Errorf("handshake read failed: %w", err)
	}
	if clientHS.Magic != protocol.Magic {
		return fmt.Errorf("bad magic from client")
	}

	// Create ZSTD encoder (reuse across requests)
	zenc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return fmt.Errorf("zstd init failed: %w", err)
	}
	defer zenc.Close()

	// Read buffer for disk reads (1MB max)
	readBuf := make([]byte, 1024*1024)

	// Disk file handle (opened on first read, path from prepare)
	var diskFile *os.File
	var diskPath string

	defer func() {
		if diskFile != nil {
			diskFile.Close()
		}
	}()

	// Watchdog: exit if no command (including ping) received within idleTimeout.
	// This handles abrupt SSH disconnects where stdin read blocks forever.
	startWatchdog()

	for {
		cmd, err := protocol.ReadCommand(reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		touchActivity()

		switch cmd {
		case protocol.CmdPing:
			// Keepalive — touchActivity() above is all we need
			continue

		case protocol.CmdPrepare:
			req, err := protocol.ReadPrepareReq(reader)
			if err != nil {
				return err
			}
			diskPath = req.DevPath

			// Flush caches
			execCmd("sh", "-c", "sync ; echo 3 > /proc/sys/vm/drop_caches")

			// Open the disk device
			if diskFile != nil {
				diskFile.Close()
			}
			diskFile, err = os.OpenFile(diskPath, os.O_RDONLY, 0)
			if err != nil {
				protocol.WriteErrorResponse(writer, fmt.Sprintf("open %s: %v", diskPath, err))
				writer.Flush()
				continue
			}
			protocol.WriteOKResponse(writer, []byte("ok"))
			writer.Flush()

		case protocol.CmdRead:
			req, err := protocol.ReadReadReq(reader)
			if err != nil {
				return err
			}
			if diskFile == nil {
				protocol.WriteErrorResponse(writer, "no disk opened")
				writer.Flush()
				continue
			}

			length := req.Length
			if length > uint32(len(readBuf)) {
				length = uint32(len(readBuf))
			}
			buf := readBuf[:length]

			n, readErr := diskFile.ReadAt(buf, int64(req.Offset))
			if n == 0 && readErr != nil {
				protocol.WriteErrorResponse(writer, fmt.Sprintf("read at %d: %v", req.Offset, readErr))
				writer.Flush()
				continue
			}
			data := buf[:n]

			// Check if all zeros
			if isAllZero(data) {
				protocol.WriteZeroResponse(writer, uint32(n))
				writer.Flush()
				continue
			}

			// Try ZSTD compression
			compressed := zenc.EncodeAll(data, nil)
			// Only use compression if it actually saves space
			if len(compressed) < n*9/10 { // at least 10% savings
				protocol.WriteCompressedResponse(writer, compressed, uint32(n))
			} else {
				protocol.WriteOKResponse(writer, data)
			}
			writer.Flush()

		case protocol.CmdBitmap:
			req, err := protocol.ReadBitmapReq(reader)
			if err != nil {
				return err
			}
			if diskFile == nil {
				protocol.WriteErrorResponse(writer, "no disk opened")
				writer.Flush()
				continue
			}
			handleBitmap(writer, zenc, diskFile, req)
			writer.Flush()

		case protocol.CmdStreamRead:
			req, err := protocol.ReadStreamReadReq(reader)
			if err != nil {
				return err
			}
			if diskFile == nil {
				protocol.WriteErrorResponse(writer, "no disk opened")
				writer.Flush()
				continue
			}
			handleStreamRead(writer, zenc, diskFile, req, readBuf)
			// no flush here — handleStreamRead flushes internally

		case protocol.CmdDiskInfo:
			disks := listDisks()
			protocol.WriteDiskInfoResponse(writer, disks)
			writer.Flush()

		case protocol.CmdClose:
			return nil

		default:
			protocol.WriteErrorResponse(writer, fmt.Sprintf("unknown cmd: 0x%02x", cmd))
			writer.Flush()
		}
	}
}

func isAllZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func execCmd(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}

func listDisks() []protocol.DiskEntry {
	out, err := execCmd("ls", "/sys/block/")
	if err != nil {
		return nil
	}
	var disks []protocol.DiskEntry
	for _, name := range strings.Fields(out) {
		if strings.HasPrefix(name, "loop") ||
			strings.HasPrefix(name, "dm-") ||
			strings.HasPrefix(name, "ram") ||
			strings.HasPrefix(name, "sr") ||
			strings.HasPrefix(name, "fd") ||
			strings.HasPrefix(name, "zram") {
			continue
		}
		d := protocol.DiskEntry{Name: "/dev/" + name}

		// Size
		sizeStr, _ := execCmd("cat", fmt.Sprintf("/sys/block/%s/size", name))
		var sectors uint64
		fmt.Sscanf(strings.TrimSpace(sizeStr), "%d", &sectors)
		d.Size = sectors * 512
		if d.Size == 0 {
			continue
		}

		// Model
		modelStr, _ := execCmd("cat", fmt.Sprintf("/sys/block/%s/device/model", name))
		d.Model = strings.TrimSpace(modelStr)

		disks = append(disks, d)
	}
	return disks
}

func handleStreamRead(w *bufio.Writer, defaultEnc *zstd.Encoder, disk *os.File, req *protocol.StreamReadReq, readBuf []byte) {
	offset := req.Offset
	remaining := req.TotalLength
	chunkSize := req.ChunkSize

	// Allocate buffer large enough for requested chunk size
	buf := readBuf
	if chunkSize > uint32(len(buf)) {
		buf = make([]byte, chunkSize)
	}

	// Select compression based on requested mode
	var zenc *zstd.Encoder
	noCompress := false
	switch req.CompressMode {
	case protocol.CompressNone:
		noCompress = true
	case protocol.CompressZSTDFast:
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			zenc = defaultEnc
		} else {
			zenc = enc
			defer enc.Close()
		}
	default:
		zenc = defaultEnc
	}

	for remaining > 0 {
		toRead := chunkSize
		if uint64(toRead) > remaining {
			toRead = uint32(remaining)
		}
		chunk := buf[:toRead]

		n, readErr := disk.ReadAt(chunk, int64(offset))
		if n == 0 && readErr != nil {
			protocol.WriteErrorResponse(w, fmt.Sprintf("stream read at %d: %v", offset, readErr))
			w.Flush()
			return
		}
		data := chunk[:n]

		if isAllZero(data) {
			protocol.WriteZeroResponse(w, uint32(n))
		} else if noCompress {
			protocol.WriteOKResponse(w, data)
		} else {
			compressed := zenc.EncodeAll(data, nil)
			if len(compressed) < n*9/10 {
				protocol.WriteCompressedResponse(w, compressed, uint32(n))
			} else {
				protocol.WriteOKResponse(w, data)
			}
		}

		offset += uint64(n)
		remaining -= uint64(n)

		// Flush to keep data flowing
		w.Flush()
	}

	// End-of-stream marker: StatusOK with 0 length
	protocol.WriteOKResponse(w, nil)
	w.Flush()
}

func handleBitmap(w *bufio.Writer, zenc *zstd.Encoder, disk *os.File, req *protocol.BitmapReq) {
	var bm *bitmap.BlockBitmap
	var err error

	switch req.FSType {
	case protocol.FSExt2, protocol.FSExt3, protocol.FSExt4:
		bm, err = bitmap.Ext4ReadBitmap(disk, req.PartOffset, req.PartSize)
	case protocol.FSXFS:
		bm, err = bitmap.XFSReadBitmap(disk, req.PartOffset, req.PartSize)
	case protocol.FSLVM:
		bm, err = readLVMBitmap(disk, req.PartOffset, req.PartSize, req.DevPath)
	case protocol.FSFat32:
		bm, err = bitmap.Fat32ReadBitmap(disk, req.PartOffset, req.PartSize)
	case protocol.FSNTFS:
		bm, err = bitmap.NTFSReadBitmap(disk, req.PartOffset, req.PartSize)
	case protocol.FSFat16:
		bm, err = bitmap.Fat16ReadBitmap(disk, req.PartOffset, req.PartSize)
	case protocol.FSSwap:
		blockSize := uint32(4096)
		totalBlocks := req.PartSize / uint64(blockSize)
		bitmapBytes := (totalBlocks + 7) / 8
		bits := make([]byte, bitmapBytes)
		if len(bits) > 0 {
			bits[0] = 1
		}
		bm = &bitmap.BlockBitmap{
			Bits:        bits,
			BlockSize:   blockSize,
			TotalBlocks: totalBlocks,
		}
	default:
		protocol.WriteErrorResponse(w, fmt.Sprintf("unsupported fs type: %d", req.FSType))
		return
	}

	if err != nil {
		protocol.WriteErrorResponse(w, fmt.Sprintf("bitmap: %v", err))
		return
	}

	meta := bitmap.EncodeMeta(bm)

	// Compress
	compressed := zenc.EncodeAll(meta, nil)
	if len(compressed) < len(meta)*9/10 {
		protocol.WriteCompressedResponse(w, compressed, uint32(len(meta)))
	} else {
		protocol.WriteOKResponse(w, meta)
	}
}
