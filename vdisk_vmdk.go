package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
)

/*
 * VMDK Sparse Extent (monolithicSparse) format:
 *
 * Layout:
 *   [Sparse Header]        512 bytes at offset 0
 *   [Descriptor]           at sector 1, ~20 sectors
 *   [Redundant GDE table]  at rgdOffset
 *   [Redundant GT tables]
 *   [Primary GDE table]    at gdOffset
 *   [Primary GT tables]
 *   [Grain data]           at start of data area
 *
 * Grain = 128 sectors = 64KB (default)
 * GT entries per table = 512
 * Grains per GDE entry = 512
 * Sectors per GDE entry = 512 * 128 = 65536 sectors = 32MB
 */

const (
	vmdkMagic          = 0x564D444B // "VMDK"
	vmdkVersion        = 1
	vmdkGrainSectors   = 128 // 64KB grains
	vmdkGTEntries      = 512
	vmdkGrainsPerGDE   = vmdkGTEntries
	vmdkSectorsPerGDE  = vmdkGrainsPerGDE * vmdkGrainSectors // 65536
	vmdkSectorSize     = 512
	vmdkDescriptorSize = 20 // sectors
)

type sparseHeader struct {
	MagicNumber        uint32
	Version            uint32
	Flags              uint32
	Capacity           uint64 // in sectors
	GrainSize          uint64 // in sectors
	DescriptorOffset   uint64
	DescriptorSize     uint64
	NumGTEsPerGT       uint32
	RGDOffset          uint64
	GDOffset           uint64
	OverHead           uint64
	UncleanShutdown    uint8
	SingleEndLineChar  byte
	NonEndLineChar     byte
	DoubleEndLineChar1 byte
	DoubleEndLineChar2 byte
	CompressAlgorithm  uint16
	Pad                [433]byte
}

type VMDKWriter struct {
	file     *os.File
	diskSize uint64
	capacity uint64 // in sectors, aligned

	numGDEntries uint32
	gdOffset     uint64 // in sectors
	rgdOffset    uint64 // in sectors
	dataOffset   uint64 // in sectors

	// GDE tables: gde[i] = sector offset of GT[i], 0 = not allocated
	gd  []uint32
	rgd []uint32
	// GT tables: gt[gde_idx][gte_idx] = sector offset of grain, 0 = sparse
	gts [][]uint32

	nextDataSector uint64 // next free sector for grain data
	grainBuf       []byte // temp buffer for grain writes
}

func NewVMDKWriter(path string, diskSize uint64) (*VMDKWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// Align capacity to grain boundary
	grainBytes := uint64(vmdkGrainSectors) * vmdkSectorSize
	capacity := ((diskSize + grainBytes - 1) / grainBytes) * uint64(vmdkGrainSectors)

	// Number of GDE entries
	numGDE := uint32((capacity + vmdkSectorsPerGDE - 1) / vmdkSectorsPerGDE)

	// Layout calculation
	// Header: 1 sector
	// Descriptor: vmdkDescriptorSize sectors
	// RGD table: ceil(numGDE * 4 / 512) sectors
	// RGD GT tables: numGDE * ceil(vmdkGTEntries * 4 / 512) sectors
	// GD table: same as RGD
	// GD GT tables: same as RGD GT
	gdTableSectors := uint64((numGDE*4 + vmdkSectorSize - 1) / vmdkSectorSize)
	gtSectors := uint64((vmdkGTEntries*4 + vmdkSectorSize - 1) / vmdkSectorSize) // per GT
	allGTSectors := uint64(numGDE) * gtSectors

	rgdOff := uint64(1 + vmdkDescriptorSize)
	rgdGTOff := rgdOff + gdTableSectors
	gdOff := rgdGTOff + allGTSectors
	gdGTOff := gdOff + gdTableSectors
	dataOff := gdGTOff + allGTSectors

	// Align dataOff to grain boundary
	dataOff = ((dataOff + uint64(vmdkGrainSectors) - 1) / uint64(vmdkGrainSectors)) * uint64(vmdkGrainSectors)

	w := &VMDKWriter{
		file:           f,
		diskSize:       diskSize,
		capacity:       capacity,
		numGDEntries:   numGDE,
		gdOffset:       gdOff,
		rgdOffset:      rgdOff,
		dataOffset:     dataOff,
		nextDataSector: dataOff,
		gd:             make([]uint32, numGDE),
		rgd:            make([]uint32, numGDE),
		gts:            make([][]uint32, numGDE),
		grainBuf:       make([]byte, grainBytes),
	}

	// Pre-allocate GT entries for RGD
	for i := uint32(0); i < numGDE; i++ {
		w.rgd[i] = uint32(rgdGTOff + uint64(i)*gtSectors)
		w.gd[i] = uint32(gdGTOff + uint64(i)*gtSectors)
		w.gts[i] = make([]uint32, vmdkGTEntries)
	}

	// Write sparse header
	hdr := sparseHeader{
		MagicNumber:        vmdkMagic,
		Version:            vmdkVersion,
		Flags:              0x03, // valid new line detection + redundant grain table
		Capacity:           capacity,
		GrainSize:          vmdkGrainSectors,
		DescriptorOffset:   1,
		DescriptorSize:     vmdkDescriptorSize,
		NumGTEsPerGT:       vmdkGTEntries,
		RGDOffset:          rgdOff,
		GDOffset:           gdOff,
		OverHead:           dataOff,
		SingleEndLineChar:  '\n',
		NonEndLineChar:     ' ',
		DoubleEndLineChar1: '\r',
		DoubleEndLineChar2: '\n',
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		return nil, err
	}

	// Write descriptor
	desc := w.buildDescriptor()
	descBuf := make([]byte, vmdkDescriptorSize*vmdkSectorSize)
	copy(descBuf, []byte(desc))
	if _, err := f.WriteAt(descBuf, vmdkSectorSize); err != nil {
		f.Close()
		return nil, err
	}

	return w, nil
}

func (w *VMDKWriter) buildDescriptor() string {
	cylinders := w.capacity / (255 * 63)
	if cylinders == 0 {
		cylinders = 1
	}
	baseName := strings.TrimSuffix(w.file.Name(), ".vmdk")
	if idx := strings.LastIndex(baseName, "/"); idx >= 0 {
		baseName = baseName[idx+1:]
	}

	var sb strings.Builder
	sb.WriteString("# Disk DescriptorFile\n")
	sb.WriteString("version=1\n")
	fmt.Fprintf(&sb, "CID=%08x\n", uint32(0xfffffffe))
	sb.WriteString("parentCID=ffffffff\n")
	sb.WriteString("createType=\"monolithicSparse\"\n")
	sb.WriteString("\n")
	sb.WriteString("# Extent description\n")
	fmt.Fprintf(&sb, "RW %d SPARSE \"%s.vmdk\"\n", w.capacity, baseName)
	sb.WriteString("\n")
	sb.WriteString("# The Disk Data Base\n")
	sb.WriteString("#DDB\n")
	sb.WriteString("ddb.virtualHWVersion = \"4\"\n")
	fmt.Fprintf(&sb, "ddb.geometry.cylinders = \"%d\"\n", cylinders)
	sb.WriteString("ddb.geometry.heads = \"255\"\n")
	sb.WriteString("ddb.geometry.sectors = \"63\"\n")
	sb.WriteString("ddb.adapterType = \"lsilogic\"\n")
	return sb.String()
}

func (w *VMDKWriter) Write(offset uint64, data []byte) error {
	grainBytes := uint64(vmdkGrainSectors) * vmdkSectorSize

	pos := uint64(0)
	for pos < uint64(len(data)) {
		absOffset := offset + pos
		grainIdx := absOffset / grainBytes
		inGrainOff := absOffset % grainBytes

		gdeIdx := grainIdx / vmdkGrainsPerGDE
		gteIdx := grainIdx % vmdkGrainsPerGDE

		if gdeIdx >= uint64(w.numGDEntries) {
			return fmt.Errorf("offset %d beyond disk capacity", absOffset)
		}

		// How much data goes into this grain
		canWrite := grainBytes - inGrainOff
		if canWrite > uint64(len(data))-pos {
			canWrite = uint64(len(data)) - pos
		}

		// Check if this write covers a full grain and is all zeros — skip it (sparse)
		if inGrainOff == 0 && canWrite == grainBytes && w.gts[gdeIdx][gteIdx] == 0 {
			if isZeroBuf(data[pos : pos+canWrite]) {
				pos += canWrite
				continue
			}
		}

		// Check if this grain is already allocated
		grainSector := w.gts[gdeIdx][gteIdx]
		if grainSector == 0 {
			// Need to allocate — but first check if the grain will be all zeros
			// Build the grain content
			for i := range w.grainBuf {
				w.grainBuf[i] = 0
			}
			copy(w.grainBuf[inGrainOff:], data[pos:pos+canWrite])

			// If entire grain is zero, skip allocation
			if isZeroBuf(w.grainBuf) {
				pos += canWrite
				continue
			}

			// Allocate new grain
			grainSector = uint32(w.nextDataSector)
			w.gts[gdeIdx][gteIdx] = grainSector
			w.nextDataSector += uint64(vmdkGrainSectors)
		} else {
			// Read existing grain data
			if _, err := w.file.ReadAt(w.grainBuf, int64(grainSector)*vmdkSectorSize); err != nil {
				for i := range w.grainBuf {
					w.grainBuf[i] = 0
				}
			}
			copy(w.grainBuf[inGrainOff:], data[pos:pos+canWrite])
		}

		// Write grain to file
		if _, err := w.file.WriteAt(w.grainBuf, int64(grainSector)*vmdkSectorSize); err != nil {
			return err
		}

		pos += canWrite
	}
	return nil
}

// isZeroBuf checks if a byte slice is all zeros
func isZeroBuf(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func (w *VMDKWriter) WriteZero(offset uint64, length uint64) error {
	// For sparse VMDK, unallocated grains are implicitly zero.
	// We need to handle partially-written grains at boundaries.

	grainBytes := uint64(vmdkGrainSectors) * vmdkSectorSize

	// If offset is grain-aligned and length is grain-aligned, nothing to do.
	// For unaligned boundaries, we need to zero-fill partial grains that
	// were previously written. Since we write sequentially, typically
	// excluded partitions haven't been written yet, so this is a no-op.

	// Handle head: if offset is not grain-aligned
	headEnd := ((offset + grainBytes - 1) / grainBytes) * grainBytes
	if headEnd > offset+length {
		headEnd = offset + length
	}
	if headEnd > offset {
		headLen := headEnd - offset
		buf := make([]byte, headLen)
		if err := w.Write(offset, buf); err != nil {
			return err
		}
		offset = headEnd
		length -= headLen
	}

	// Middle: whole grains — skip (sparse)
	wholeGrains := length / grainBytes
	offset += wholeGrains * grainBytes
	length -= wholeGrains * grainBytes

	// Handle tail: if remaining length
	if length > 0 {
		buf := make([]byte, length)
		if err := w.Write(offset, buf); err != nil {
			return err
		}
	}
	return nil
}

func (w *VMDKWriter) Close() error {
	// Write GD and RGD tables
	for i := uint32(0); i < w.numGDEntries; i++ {
		// Write RGD table entry
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], w.rgd[i])
		w.file.WriteAt(buf[:], int64(w.rgdOffset)*vmdkSectorSize+int64(i)*4)

		// Write GD table entry
		binary.LittleEndian.PutUint32(buf[:], w.gd[i])
		w.file.WriteAt(buf[:], int64(w.gdOffset)*vmdkSectorSize+int64(i)*4)

		// Write GT entries for both RGD and GD
		gtBuf := make([]byte, vmdkGTEntries*4)
		for j := 0; j < vmdkGTEntries; j++ {
			binary.LittleEndian.PutUint32(gtBuf[j*4:], w.gts[i][j])
		}
		w.file.WriteAt(gtBuf, int64(w.rgd[i])*vmdkSectorSize)
		w.file.WriteAt(gtBuf, int64(w.gd[i])*vmdkSectorSize)
	}

	// Write end-of-stream marker (footer)
	footer := sparseHeader{
		MagicNumber:        vmdkMagic,
		Version:            vmdkVersion,
		Flags:              0x03,
		Capacity:           w.capacity,
		GrainSize:          vmdkGrainSectors,
		DescriptorOffset:   0,
		DescriptorSize:     0,
		NumGTEsPerGT:       vmdkGTEntries,
		RGDOffset:          0,
		GDOffset:           0,
		OverHead:           w.dataOffset,
		SingleEndLineChar:  '\n',
		NonEndLineChar:     ' ',
		DoubleEndLineChar1: '\r',
		DoubleEndLineChar2: '\n',
	}
	w.file.Seek(0, 2) // seek to end
	binary.Write(w.file, binary.LittleEndian, &footer)

	return w.file.Close()
}
