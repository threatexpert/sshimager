package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

/*
 * VDI (VirtualBox Disk Image) format:
 *
 * Layout:
 *   [Pre-header]         64 bytes (signature + version)
 *   [Header]             from offset 64 (~400 bytes)
 *   [Block Map]          at offsetBlocks
 *   [Data]               at offsetData, each block = 1MB
 */

const (
	vdiSignature  = 0xBEDA107F
	vdiVersion    = 0x00010001
	vdiTypeDyn    = 1
	vdiBlockSize  = 1024 * 1024 // 1MB
	vdiSectorSize = 512
	vdiBlockFree  = 0xFFFFFFFF
)

type VDIWriter struct {
	file      *os.File
	diskSize  uint64
	numBlocks uint32
	blockMap  []uint32
	nextBlock uint32

	offsetBlocks uint32 // file offset to block map
	offsetData   uint32 // file offset to first data block
}

func NewVDIWriter(path string, diskSize uint64) (*VDIWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	numBlocks := uint32((diskSize + vdiBlockSize - 1) / vdiBlockSize)

	// Header layout
	// Pre-header: 64 bytes
	// Header body: starts at 64, ~400 bytes, padded to 512
	headerSize := uint32(512)
	offsetBlocks := headerSize
	blockMapSize := numBlocks * 4
	blockMapPadded := ((blockMapSize + vdiSectorSize - 1) / vdiSectorSize) * vdiSectorSize
	offsetData := offsetBlocks + blockMapPadded

	w := &VDIWriter{
		file:         f,
		diskSize:     diskSize,
		numBlocks:    numBlocks,
		blockMap:     make([]uint32, numBlocks),
		offsetBlocks: offsetBlocks,
		offsetData:   offsetData,
	}
	for i := range w.blockMap {
		w.blockMap[i] = vdiBlockFree
	}

	// Write header
	w.writeHeader()

	// Write empty block map
	mapBuf := make([]byte, blockMapPadded)
	for i := range mapBuf {
		mapBuf[i] = 0xFF
	}
	f.WriteAt(mapBuf, int64(offsetBlocks))

	return w, nil
}

func (w *VDIWriter) writeHeader() {
	buf := make([]byte, 512)

	// Pre-header (offset 0)
	copy(buf[0:], "<<< Oracle VM VirtualBox Disk Image >>>\n")
	binary.LittleEndian.PutUint32(buf[64:], vdiSignature) // signature
	binary.LittleEndian.PutUint32(buf[68:], vdiVersion)   // version

	// Header body (offset 72)
	binary.LittleEndian.PutUint32(buf[72:], 400)               // header size (bytes)
	binary.LittleEndian.PutUint32(buf[76:], vdiTypeDyn)         // image type: dynamic
	binary.LittleEndian.PutUint32(buf[80:], 0)                  // image flags
	binary.LittleEndian.PutUint32(buf[340:], w.offsetBlocks)    // offset to blocks
	binary.LittleEndian.PutUint32(buf[344:], w.offsetData)      // offset to data
	binary.LittleEndian.PutUint32(buf[348:], 0)                 // geometry: cylinders
	binary.LittleEndian.PutUint32(buf[352:], 0)                 // geometry: heads
	binary.LittleEndian.PutUint32(buf[356:], 0)                 // geometry: sectors
	binary.LittleEndian.PutUint32(buf[360:], vdiSectorSize)     // sector size
	binary.LittleEndian.PutUint64(buf[368:], w.diskSize)        // disk size
	binary.LittleEndian.PutUint32(buf[376:], vdiBlockSize)      // block size
	binary.LittleEndian.PutUint32(buf[380:], 0)                 // block extra data
	binary.LittleEndian.PutUint32(buf[384:], w.numBlocks)       // blocks in image
	binary.LittleEndian.PutUint32(buf[388:], 0)                 // blocks allocated

	// UUID (16 bytes at offset 392)
	copy(buf[392:], []byte{0xd2, 0x4d, 0x4b, 0x02, 0x03, 0x04, 0x05, 0x06,
		0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e})

	w.file.WriteAt(buf, 0)
}

func (w *VDIWriter) Write(offset uint64, data []byte) error {
	pos := uint64(0)
	for pos < uint64(len(data)) {
		absOff := offset + pos
		blockIdx := uint32(absOff / vdiBlockSize)
		inBlockOff := absOff % vdiBlockSize

		if blockIdx >= w.numBlocks {
			return fmt.Errorf("offset %d beyond disk capacity", absOff)
		}

		canWrite := uint64(vdiBlockSize) - inBlockOff
		if canWrite > uint64(len(data))-pos {
			canWrite = uint64(len(data)) - pos
		}

		// Skip all-zero full blocks (keep sparse)
		if inBlockOff == 0 && canWrite == vdiBlockSize && w.blockMap[blockIdx] == vdiBlockFree {
			if isZeroBuf(data[pos : pos+canWrite]) {
				pos += canWrite
				continue
			}
		}

		// Allocate block if needed
		if w.blockMap[blockIdx] == vdiBlockFree {
			w.blockMap[blockIdx] = w.nextBlock
			w.nextBlock++
		}

		fileOff := int64(w.offsetData) + int64(w.blockMap[blockIdx])*vdiBlockSize + int64(inBlockOff)

		if _, err := w.file.WriteAt(data[pos:pos+canWrite], fileOff); err != nil {
			return err
		}
		pos += canWrite
	}
	return nil
}

func (w *VDIWriter) WriteZero(offset uint64, length uint64) error {
	// Unallocated blocks are implicitly zero in VDI
	return nil
}

func (w *VDIWriter) Close() error {
	// Write block map
	mapBuf := make([]byte, w.numBlocks*4)
	for i := uint32(0); i < w.numBlocks; i++ {
		binary.LittleEndian.PutUint32(mapBuf[i*4:], w.blockMap[i])
	}
	w.file.WriteAt(mapBuf, int64(w.offsetBlocks))

	// Update blocks allocated count in header
	var countBuf [4]byte
	binary.LittleEndian.PutUint32(countBuf[:], w.nextBlock)
	w.file.WriteAt(countBuf[:], 388)

	return w.file.Close()
}
