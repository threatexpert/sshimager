package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

// NTFSReadBitmap reads the NTFS $Bitmap file (MFT record #6) to build a used-cluster bitmap.
//
// NTFS layout:
//   Boot sector (sector 0) → BPB + NTFS parameters
//   MFT at MFT_Start_Cluster → array of MFT records
//   $Bitmap (MFT record #6) → cluster allocation bitmap (bit=1 = used)
//
// Steps:
//   1. Parse boot sector for cluster size, MFT location, MFT record size
//   2. Read MFT record #6 ($Bitmap), apply fixup
//   3. Find $DATA attribute (type 0x80), parse data runs
//   4. Read bitmap data from the data runs
func NTFSReadBitmap(r io.ReaderAt, partOffset, partSize uint64) (*BlockBitmap, error) {
	// Read boot sector
	bs := make([]byte, 512)
	if err := readAt(r, bs, int64(partOffset)); err != nil {
		return nil, fmt.Errorf("ntfs: cannot read boot sector: %w", err)
	}

	if string(bs[3:7]) != "NTFS" {
		return nil, fmt.Errorf("ntfs: bad OEM ID")
	}

	bytesPerSec := binary.LittleEndian.Uint16(bs[0x0B:0x0D])
	secPerClus := bs[0x0D]
	if bytesPerSec == 0 || secPerClus == 0 {
		return nil, fmt.Errorf("ntfs: invalid BPB")
	}
	clusterSize := uint32(bytesPerSec) * uint32(secPerClus)

	totalSectors := binary.LittleEndian.Uint64(bs[0x28:0x30])
	totalClusters := totalSectors / uint64(secPerClus)

	mftCluster := binary.LittleEndian.Uint64(bs[0x30:0x38])

	// MFT record size: bs[0x40] is signed. If positive, it's clusters per record.
	// If negative, record size = 2^(-value) bytes.
	mftRecordSize := uint32(0)
	rawRecSize := int8(bs[0x40])
	if rawRecSize > 0 {
		mftRecordSize = uint32(rawRecSize) * clusterSize
	} else {
		mftRecordSize = 1 << uint32(-rawRecSize)
	}
	if mftRecordSize < 512 || mftRecordSize > 65536 {
		return nil, fmt.Errorf("ntfs: invalid MFT record size: %d", mftRecordSize)
	}

	// Read MFT record #6 ($Bitmap)
	mftOffset := partOffset + mftCluster*uint64(clusterSize)
	bitmapRecordOffset := mftOffset + 6*uint64(mftRecordSize)

	record := make([]byte, mftRecordSize)
	if err := readAt(r, record, int64(bitmapRecordOffset)); err != nil {
		return nil, fmt.Errorf("ntfs: cannot read $Bitmap MFT record: %w", err)
	}

	// Verify "FILE" signature
	if string(record[0:4]) != "FILE" {
		return nil, fmt.Errorf("ntfs: $Bitmap record bad magic: %q", string(record[0:4]))
	}

	// Apply Update Sequence Array fixup
	if err := ntfsFixupRecord(record); err != nil {
		return nil, fmt.Errorf("ntfs: $Bitmap fixup: %w", err)
	}

	// Find $DATA attribute (type 0x80)
	attrOffset := binary.LittleEndian.Uint16(record[0x14:0x16])
	if attrOffset >= uint16(mftRecordSize) {
		return nil, fmt.Errorf("ntfs: invalid first attribute offset")
	}

	var dataRuns []byte
	var dataSize uint64
	pos := uint32(attrOffset)

	for pos+4 <= mftRecordSize {
		attrType := binary.LittleEndian.Uint32(record[pos:])
		if attrType == 0xFFFFFFFF {
			break // end of attributes
		}
		attrLen := binary.LittleEndian.Uint32(record[pos+4:])
		if attrLen == 0 || pos+attrLen > mftRecordSize {
			break
		}

		if attrType == 0x80 { // $DATA
			nonResident := record[pos+8]
			if nonResident == 0 {
				// Resident $DATA — bitmap is embedded in the MFT record
				contentLen := binary.LittleEndian.Uint32(record[pos+0x10:])
				contentOff := binary.LittleEndian.Uint16(record[pos+0x14:])
				start := pos + uint32(contentOff)
				end := start + contentLen
				if end > mftRecordSize {
					return nil, fmt.Errorf("ntfs: resident $DATA overflow")
				}
				bitmapData := record[start:end]
				return ntfsBuildBitmap(bitmapData, clusterSize, totalClusters)
			}
			// Non-resident $DATA
			dataSize = binary.LittleEndian.Uint64(record[pos+0x30:]) // real size
			runOff := binary.LittleEndian.Uint16(record[pos+0x20:])
			dataRuns = record[pos+uint32(runOff) : pos+attrLen]
			break
		}
		pos += attrLen
	}

	if dataRuns == nil {
		return nil, fmt.Errorf("ntfs: $DATA attribute not found in $Bitmap record")
	}

	// Parse data runs and read bitmap data
	bitmapData, err := ntfsReadDataRuns(r, partOffset, clusterSize, dataRuns, dataSize)
	if err != nil {
		return nil, fmt.Errorf("ntfs: read $Bitmap data: %w", err)
	}

	return ntfsBuildBitmap(bitmapData, clusterSize, totalClusters)
}

// ntfsBuildBitmap wraps raw NTFS bitmap bytes into BlockBitmap.
// NTFS $Bitmap is already bit-per-cluster, bit=1 means used — same as our format.
func ntfsBuildBitmap(bitmapData []byte, clusterSize uint32, totalClusters uint64) (*BlockBitmap, error) {
	needBytes := (totalClusters + 7) / 8
	bits := make([]byte, needBytes)
	// Copy available bitmap data
	n := uint64(len(bitmapData))
	if n > needBytes {
		n = needBytes
	}
	copy(bits, bitmapData[:n])

	// If bitmap is shorter than total clusters, assume remaining clusters are used
	if n < needBytes {
		for i := n; i < needBytes; i++ {
			bits[i] = 0xFF
		}
		// Fix the last byte if totalClusters is not byte-aligned
		if tail := totalClusters % 8; tail > 0 {
			bits[needBytes-1] = (1 << tail) - 1
		}
	}

	return &BlockBitmap{
		Bits:        bits,
		BlockSize:   clusterSize,
		TotalBlocks: totalClusters,
	}, nil
}

// ntfsFixupRecord applies the NTFS Update Sequence Array fixup.
// Each 512-byte sector's last 2 bytes are replaced by the USN during write;
// we must restore them from the USA for the record to be readable.
func ntfsFixupRecord(record []byte) error {
	if len(record) < 48 {
		return fmt.Errorf("record too short")
	}
	usaOffset := binary.LittleEndian.Uint16(record[0x04:0x06])
	usaCount := binary.LittleEndian.Uint16(record[0x06:0x08]) // includes the USN itself

	if usaCount < 2 {
		return nil // nothing to fix
	}
	if uint32(usaOffset)+uint32(usaCount)*2 > uint32(len(record)) {
		return fmt.Errorf("USA extends beyond record")
	}

	// usn := binary.LittleEndian.Uint16(record[usaOffset:])

	// For each sector (starting at sector 1), restore the last 2 bytes
	for i := uint16(1); i < usaCount; i++ {
		sectorEnd := int(i) * 512 // offset of last 2 bytes of sector i
		if sectorEnd+1 >= len(record) {
			break
		}
		replaceOff := int(usaOffset) + int(i)*2
		if replaceOff+1 >= len(record) {
			break
		}
		// Restore original bytes from USA
		record[sectorEnd-2] = record[replaceOff]
		record[sectorEnd-1] = record[replaceOff+1]
	}
	return nil
}

// ntfsReadDataRuns parses NTFS data run list and reads the actual data.
// Data run encoding: each run starts with a header byte where
//   low nibble  = number of bytes for run length
//   high nibble = number of bytes for run offset (signed, relative to previous)
// Followed by length bytes (LE), then offset bytes (LE, signed delta).
func ntfsReadDataRuns(r io.ReaderAt, partOffset uint64, clusterSize uint32, runs []byte, totalSize uint64) ([]byte, error) {
	result := make([]byte, totalSize)
	pos := 0
	resultOff := uint64(0)
	prevLCN := int64(0)

	for pos < len(runs) {
		header := runs[pos]
		if header == 0 {
			break // end of run list
		}
		pos++

		lenBytes := int(header & 0x0F)
		offBytes := int((header >> 4) & 0x0F)

		if lenBytes == 0 || pos+lenBytes+offBytes > len(runs) {
			break
		}

		// Read run length (unsigned)
		runLen := uint64(0)
		for i := 0; i < lenBytes; i++ {
			runLen |= uint64(runs[pos+i]) << (uint(i) * 8)
		}
		pos += lenBytes

		// Read run offset (signed delta from previous LCN)
		if offBytes == 0 {
			// Sparse run — fill with zeros (already zero in result)
			advance := runLen * uint64(clusterSize)
			resultOff += advance
			continue
		}

		runOff := int64(0)
		for i := 0; i < offBytes; i++ {
			runOff |= int64(runs[pos+i]) << (uint(i) * 8)
		}
		// Sign-extend
		if runs[pos+offBytes-1]&0x80 != 0 {
			for i := offBytes; i < 8; i++ {
				runOff |= int64(0xFF) << (uint(i) * 8)
			}
		}
		pos += offBytes

		lcn := prevLCN + runOff
		prevLCN = lcn

		// Read data from this run
		diskOff := partOffset + uint64(lcn)*uint64(clusterSize)
		readSize := runLen * uint64(clusterSize)
		if resultOff+readSize > totalSize {
			readSize = totalSize - resultOff
		}
		if readSize > 0 {
			if err := readAt(r, result[resultOff:resultOff+readSize], int64(diskOff)); err != nil {
				return nil, fmt.Errorf("read data run at LCN %d: %w", lcn, err)
			}
		}
		resultOff += runLen * uint64(clusterSize)
		if resultOff >= totalSize {
			break
		}
	}

	return result, nil
}
