package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

/*
 * VHD (Virtual Hard Disk) Dynamic format:
 *
 * Layout:
 *   [Copy of Footer]   512 bytes at offset 0
 *   [Dynamic Header]   1024 bytes
 *   [BAT]              Block Allocation Table
 *   [Data blocks]      Each block = bitmap + data sectors
 *   [Footer]           512 bytes at end of file
 *
 * Default block size: 2MB
 * Each data block has a sector bitmap (512 bytes for 2MB block) + data
 */

const (
	vhdCookie      = "conectix"
	vhdDynCookie   = "cxsparse"
	vhdBlockSize   = 2 * 1024 * 1024 // 2MB
	vhdSectorSize  = 512
	vhdBitmapBytes = vhdBlockSize / vhdSectorSize / 8 // 512 bytes for 2MB block
	vhdBitmapPad   = 512                               // round bitmap to sector
	vhdVersion     = 0x00010000
	vhdTypeDynamic = 3
	vhdFeatures    = 0x00000002 // reserved bit
	vhdCreatorApp  = "sshi"
	vhdBATNoAlloc  = 0xFFFFFFFF
)

// VHD footer (big-endian)
type vhdFooter struct {
	Cookie         [8]byte
	Features       uint32
	FileFormatVer  uint32
	DataOffset     uint64
	TimeStamp      uint32
	CreatorApp     [4]byte
	CreatorVer     uint32
	CreatorHostOS  uint32
	OrigSize       uint64
	CurrSize       uint64
	DiskGeometry   uint32
	DiskType       uint32
	Checksum       uint32
	UniqueID       [16]byte
	SavedState     byte
	Reserved       [427]byte
}

type vhdDynHeader struct {
	Cookie         [8]byte
	DataOffset     uint64
	TableOffset    uint64
	HeaderVersion  uint32
	MaxTableEntry  uint32
	BlockSize      uint32
	Checksum       uint32
	ParentUniqueID [16]byte
	ParentTimeStamp uint32
	Reserved1      uint32
	ParentUnicodeName [512]byte
	ParentLocators [8 * 24]byte
	Reserved2      [256]byte
}

type VHDWriter struct {
	file      *os.File
	diskSize  uint64
	numBlocks uint32
	batOffset uint64
	dataStart uint64

	bat          []uint32
	nextBlockOff uint64 // next free offset for data block (in bytes)
}

func vhdChecksum(data []byte) uint32 {
	var sum uint32
	for _, b := range data {
		sum += uint32(b)
	}
	return ^sum
}

func vhdGeometry(totalSectors uint64) uint32 {
	// VHD specification CHS calculation algorithm
	var cyls, heads, spt uint64

	if totalSectors > 65535*16*255 {
		totalSectors = 65535 * 16 * 255
	}

	if totalSectors >= 65535*16*63 {
		spt = 255
		heads = 16
	} else {
		spt = 17
		cylTimesHeads := totalSectors / spt
		heads = (cylTimesHeads + 1023) / 1024

		if heads < 4 {
			heads = 4
		}
		if cylTimesHeads >= heads*1024 || heads > 16 {
			spt = 31
			heads = 16
			cylTimesHeads = totalSectors / spt
		}
		if cylTimesHeads >= heads*1024 {
			spt = 63
			heads = 16
			cylTimesHeads = totalSectors / spt
		}
	}

	cyls = totalSectors / (heads * spt)
	if cyls > 65535 {
		cyls = 65535
	}
	if cyls == 0 {
		cyls = 1
	}

	return uint32((cyls << 16) | (heads << 8) | spt)
}

func NewVHDWriter(path string, diskSize uint64) (*VHDWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	numBlocks := uint32((diskSize + vhdBlockSize - 1) / vhdBlockSize)
	batSize := uint64(numBlocks) * 4
	batSectors := (batSize + vhdSectorSize - 1) / vhdSectorSize
	batPadded := batSectors * vhdSectorSize

	// Layout: footer_copy(512) + dyn_header(1024) + BAT + data + footer(512)
	batOffset := uint64(512 + 1024)
	dataStart := batOffset + batPadded
	// Align data start to 512
	dataStart = ((dataStart + vhdSectorSize - 1) / vhdSectorSize) * vhdSectorSize

	w := &VHDWriter{
		file:         f,
		diskSize:     diskSize,
		numBlocks:    numBlocks,
		batOffset:    batOffset,
		dataStart:    dataStart,
		bat:          make([]uint32, numBlocks),
		nextBlockOff: dataStart,
	}
	for i := range w.bat {
		w.bat[i] = vhdBATNoAlloc
	}

	// Write footer copy at offset 0
	footer := w.buildFooter()
	footerBytes := w.encodeFooter(footer)
	f.WriteAt(footerBytes, 0)

	// Write dynamic header
	dynHdr := w.buildDynHeader()
	dynBytes := w.encodeDynHeader(dynHdr)
	f.WriteAt(dynBytes, 512)

	// Write empty BAT
	batBuf := make([]byte, batPadded)
	for i := range batBuf {
		batBuf[i] = 0xFF // all 0xFF = not allocated
	}
	f.WriteAt(batBuf, int64(batOffset))

	return w, nil
}

func (w *VHDWriter) buildFooter() vhdFooter {
	var ft vhdFooter
	copy(ft.Cookie[:], vhdCookie)
	ft.Features = vhdFeatures
	ft.FileFormatVer = vhdVersion
	ft.DataOffset = 512 // dynamic header offset
	// VHD timestamp: seconds since 2000-01-01 00:00:00 UTC
	epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	ft.TimeStamp = uint32(time.Now().UTC().Sub(epoch).Seconds())
	copy(ft.CreatorApp[:], vhdCreatorApp)
	ft.CreatorVer = 0x00010000
	ft.CreatorHostOS = 0x5769326B // "Wi2k"
	ft.OrigSize = w.diskSize
	ft.CurrSize = w.diskSize
	ft.DiskGeometry = vhdGeometry(w.diskSize / vhdSectorSize)
	ft.DiskType = vhdTypeDynamic
	// TODO: generate proper UUID
	ft.UniqueID = [16]byte{0xd2, 0x4d, 0x4b, 0x01, 0x02, 0x03, 0x04, 0x05,
		0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d}
	return ft
}

func (w *VHDWriter) encodeFooter(ft vhdFooter) []byte {
	buf := make([]byte, 512)
	ft.Checksum = 0
	// Encode without checksum
	copy(buf[0:8], ft.Cookie[:])
	binary.BigEndian.PutUint32(buf[8:], ft.Features)
	binary.BigEndian.PutUint32(buf[12:], ft.FileFormatVer)
	binary.BigEndian.PutUint64(buf[16:], ft.DataOffset)
	binary.BigEndian.PutUint32(buf[24:], ft.TimeStamp)
	copy(buf[28:32], ft.CreatorApp[:])
	binary.BigEndian.PutUint32(buf[32:], ft.CreatorVer)
	binary.BigEndian.PutUint32(buf[36:], ft.CreatorHostOS)
	binary.BigEndian.PutUint64(buf[40:], ft.OrigSize)
	binary.BigEndian.PutUint64(buf[48:], ft.CurrSize)
	binary.BigEndian.PutUint32(buf[56:], ft.DiskGeometry)
	binary.BigEndian.PutUint32(buf[60:], ft.DiskType)
	// checksum at [64:68]
	copy(buf[68:84], ft.UniqueID[:])
	buf[84] = ft.SavedState
	// compute checksum
	binary.BigEndian.PutUint32(buf[64:], vhdChecksum(buf))
	return buf
}

func (w *VHDWriter) buildDynHeader() vhdDynHeader {
	var dh vhdDynHeader
	copy(dh.Cookie[:], vhdDynCookie)
	dh.DataOffset = 0xFFFFFFFFFFFFFFFF // no next structure
	dh.TableOffset = w.batOffset
	dh.HeaderVersion = vhdVersion
	dh.MaxTableEntry = w.numBlocks
	dh.BlockSize = vhdBlockSize
	return dh
}

func (w *VHDWriter) encodeDynHeader(dh vhdDynHeader) []byte {
	buf := make([]byte, 1024)
	copy(buf[0:8], dh.Cookie[:])
	binary.BigEndian.PutUint64(buf[8:], dh.DataOffset)
	binary.BigEndian.PutUint64(buf[16:], dh.TableOffset)
	binary.BigEndian.PutUint32(buf[24:], dh.HeaderVersion)
	binary.BigEndian.PutUint32(buf[28:], dh.MaxTableEntry)
	binary.BigEndian.PutUint32(buf[32:], dh.BlockSize)
	// checksum at [36:40]
	binary.BigEndian.PutUint32(buf[36:], vhdChecksum(buf))
	return buf
}

func (w *VHDWriter) allocBlock(blockIdx uint32) (uint64, error) {
	if w.bat[blockIdx] != vhdBATNoAlloc {
		return uint64(w.bat[blockIdx]) * vhdSectorSize, nil
	}

	blockOff := w.nextBlockOff
	sectorOff := blockOff / vhdSectorSize
	if sectorOff > 0xFFFFFFFF {
		return 0, fmt.Errorf("VHD file too large")
	}
	w.bat[blockIdx] = uint32(sectorOff)

	// Write sector bitmap (all 1s = all sectors present)
	bitmap := make([]byte, vhdBitmapPad)
	for i := range bitmap {
		bitmap[i] = 0xFF
	}
	w.file.WriteAt(bitmap, int64(blockOff))

	w.nextBlockOff = blockOff + uint64(vhdBitmapPad) + vhdBlockSize
	return blockOff, nil
}

func (w *VHDWriter) Write(offset uint64, data []byte) error {
	pos := uint64(0)
	for pos < uint64(len(data)) {
		absOff := offset + pos
		blockIdx := uint32(absOff / vhdBlockSize)
		inBlockOff := absOff % vhdBlockSize

		if blockIdx >= w.numBlocks {
			return fmt.Errorf("offset %d beyond disk capacity", absOff)
		}

		canWrite := vhdBlockSize - inBlockOff
		if canWrite > uint64(len(data))-pos {
			canWrite = uint64(len(data)) - pos
		}

		// Skip all-zero full blocks (keep sparse)
		if inBlockOff == 0 && canWrite == vhdBlockSize && w.bat[blockIdx] == vhdBATNoAlloc {
			if isZeroBuf(data[pos : pos+canWrite]) {
				pos += canWrite
				continue
			}
		}

		blockStart, err := w.allocBlock(blockIdx)
		if err != nil {
			return err
		}

		fileOff := int64(blockStart) + int64(vhdBitmapPad) + int64(inBlockOff)
		if _, err := w.file.WriteAt(data[pos:pos+canWrite], fileOff); err != nil {
			return err
		}
		pos += canWrite
	}
	return nil
}

func (w *VHDWriter) WriteZero(offset uint64, length uint64) error {
	// Unallocated blocks in VHD are implicitly zero
	return nil
}

func (w *VHDWriter) Close() error {
	// Write BAT
	batBuf := make([]byte, w.numBlocks*4)
	for i := uint32(0); i < w.numBlocks; i++ {
		binary.BigEndian.PutUint32(batBuf[i*4:], w.bat[i])
	}
	w.file.WriteAt(batBuf, int64(w.batOffset))

	// Write footer at end
	footer := w.buildFooter()
	footerBytes := w.encodeFooter(footer)
	w.file.Seek(0, 2) // seek to end
	// Pad to sector boundary if needed
	pos, _ := w.file.Seek(0, 1)
	if pad := vhdSectorSize - (pos % vhdSectorSize); pad > 0 && pad < vhdSectorSize {
		w.file.Write(make([]byte, pad))
	}
	w.file.Write(footerBytes)

	return w.file.Close()
}
