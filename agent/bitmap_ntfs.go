package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

func readNTFSBitmap(r io.ReaderAt, partOffset, partSize uint64) ([]byte, uint32, uint64, error) {
	// Read boot sector
	bs := make([]byte, 512)
	if err := readFullAt(r, bs, int64(partOffset)); err != nil {
		return nil, 0, 0, fmt.Errorf("ntfs: cannot read boot sector: %w", err)
	}

	if string(bs[3:7]) != "NTFS" {
		return nil, 0, 0, fmt.Errorf("ntfs: bad OEM ID")
	}

	bytesPerSec := binary.LittleEndian.Uint16(bs[0x0B:0x0D])
	secPerClus := bs[0x0D]
	if bytesPerSec == 0 || secPerClus == 0 {
		return nil, 0, 0, fmt.Errorf("ntfs: invalid BPB")
	}
	clusterSize := uint32(bytesPerSec) * uint32(secPerClus)

	totalSectors := binary.LittleEndian.Uint64(bs[0x28:0x30])
	totalClusters := totalSectors / uint64(secPerClus)

	mftCluster := binary.LittleEndian.Uint64(bs[0x30:0x38])

	// MFT record size
	mftRecordSize := uint32(0)
	rawRecSize := int8(bs[0x40])
	if rawRecSize > 0 {
		mftRecordSize = uint32(rawRecSize) * clusterSize
	} else {
		mftRecordSize = 1 << uint32(-rawRecSize)
	}
	if mftRecordSize < 512 || mftRecordSize > 65536 {
		return nil, 0, 0, fmt.Errorf("ntfs: invalid MFT record size: %d", mftRecordSize)
	}

	// Read MFT record #6 ($Bitmap)
	mftOffset := partOffset + mftCluster*uint64(clusterSize)
	bitmapRecordOffset := mftOffset + 6*uint64(mftRecordSize)

	record := make([]byte, mftRecordSize)
	if err := readFullAt(r, record, int64(bitmapRecordOffset)); err != nil {
		return nil, 0, 0, fmt.Errorf("ntfs: cannot read $Bitmap MFT record: %w", err)
	}

	if string(record[0:4]) != "FILE" {
		return nil, 0, 0, fmt.Errorf("ntfs: $Bitmap record bad magic")
	}

	// Apply fixup
	if err := ntfsFixup(record); err != nil {
		return nil, 0, 0, fmt.Errorf("ntfs: fixup: %w", err)
	}

	// Find $DATA attribute (type 0x80)
	attrOffset := binary.LittleEndian.Uint16(record[0x14:0x16])
	pos := uint32(attrOffset)

	var dataRuns []byte
	var dataSize uint64

	for pos+4 <= mftRecordSize {
		attrType := binary.LittleEndian.Uint32(record[pos:])
		if attrType == 0xFFFFFFFF {
			break
		}
		attrLen := binary.LittleEndian.Uint32(record[pos+4:])
		if attrLen == 0 || pos+attrLen > mftRecordSize {
			break
		}

		if attrType == 0x80 { // $DATA
			nonResident := record[pos+8]
			if nonResident == 0 {
				// Resident
				contentLen := binary.LittleEndian.Uint32(record[pos+0x10:])
				contentOff := binary.LittleEndian.Uint16(record[pos+0x14:])
				start := pos + uint32(contentOff)
				end := start + contentLen
				if end > mftRecordSize {
					return nil, 0, 0, fmt.Errorf("ntfs: resident $DATA overflow")
				}
				return ntfsMakeBitmap(record[start:end], clusterSize, totalClusters)
			}
			dataSize = binary.LittleEndian.Uint64(record[pos+0x30:])
			runOff := binary.LittleEndian.Uint16(record[pos+0x20:])
			dataRuns = record[pos+uint32(runOff) : pos+attrLen]
			break
		}
		pos += attrLen
	}

	if dataRuns == nil {
		return nil, 0, 0, fmt.Errorf("ntfs: $DATA attribute not found")
	}

	bitmapData, err := ntfsParseDataRuns(r, partOffset, clusterSize, dataRuns, dataSize)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("ntfs: read $Bitmap data: %w", err)
	}

	return ntfsMakeBitmap(bitmapData, clusterSize, totalClusters)
}

func ntfsMakeBitmap(bitmapData []byte, clusterSize uint32, totalClusters uint64) ([]byte, uint32, uint64, error) {
	needBytes := (totalClusters + 7) / 8
	bits := make([]byte, needBytes)
	n := uint64(len(bitmapData))
	if n > needBytes {
		n = needBytes
	}
	copy(bits, bitmapData[:n])

	// Assume remaining clusters are used if bitmap is shorter
	if n < needBytes {
		for i := n; i < needBytes; i++ {
			bits[i] = 0xFF
		}
		if tail := totalClusters % 8; tail > 0 {
			bits[needBytes-1] = (1 << tail) - 1
		}
	}

	return bits, clusterSize, totalClusters, nil
}

func ntfsFixup(record []byte) error {
	if len(record) < 48 {
		return fmt.Errorf("record too short")
	}
	usaOffset := binary.LittleEndian.Uint16(record[0x04:0x06])
	usaCount := binary.LittleEndian.Uint16(record[0x06:0x08])
	if usaCount < 2 {
		return nil
	}
	if uint32(usaOffset)+uint32(usaCount)*2 > uint32(len(record)) {
		return fmt.Errorf("USA extends beyond record")
	}

	for i := uint16(1); i < usaCount; i++ {
		sectorEnd := int(i) * 512
		if sectorEnd+1 >= len(record) {
			break
		}
		replaceOff := int(usaOffset) + int(i)*2
		if replaceOff+1 >= len(record) {
			break
		}
		record[sectorEnd-2] = record[replaceOff]
		record[sectorEnd-1] = record[replaceOff+1]
	}
	return nil
}

func ntfsParseDataRuns(r io.ReaderAt, partOffset uint64, clusterSize uint32, runs []byte, totalSize uint64) ([]byte, error) {
	result := make([]byte, totalSize)
	pos := 0
	resultOff := uint64(0)
	prevLCN := int64(0)

	for pos < len(runs) {
		header := runs[pos]
		if header == 0 {
			break
		}
		pos++

		lenBytes := int(header & 0x0F)
		offBytes := int((header >> 4) & 0x0F)

		if lenBytes == 0 || pos+lenBytes+offBytes > len(runs) {
			break
		}

		runLen := uint64(0)
		for i := 0; i < lenBytes; i++ {
			runLen |= uint64(runs[pos+i]) << (uint(i) * 8)
		}
		pos += lenBytes

		if offBytes == 0 {
			// Sparse run
			resultOff += runLen * uint64(clusterSize)
			continue
		}

		runOff := int64(0)
		for i := 0; i < offBytes; i++ {
			runOff |= int64(runs[pos+i]) << (uint(i) * 8)
		}
		if runs[pos+offBytes-1]&0x80 != 0 {
			for i := offBytes; i < 8; i++ {
				runOff |= int64(0xFF) << (uint(i) * 8)
			}
		}
		pos += offBytes

		lcn := prevLCN + runOff
		prevLCN = lcn

		diskOff := partOffset + uint64(lcn)*uint64(clusterSize)
		readSize := runLen * uint64(clusterSize)
		if resultOff+readSize > totalSize {
			readSize = totalSize - resultOff
		}
		if readSize > 0 {
			if err := readFullAt(r, result[resultOff:resultOff+readSize], int64(diskOff)); err != nil {
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
