package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Protocol magic and version for handshake
var Magic = [4]byte{'S', 'S', 'H', 'I'}

const Version uint16 = 1

// Command bytes
const (
	CmdRead       byte = 0x01
	CmdBitmap     byte = 0x02
	CmdDiskInfo   byte = 0x03
	CmdPrepare    byte = 0x04
	CmdStreamRead byte = 0x05
	CmdPing       byte = 0x06
	CmdClose      byte = 0xFF
)

// Response status bytes
const (
	StatusOK         byte = 0x00
	StatusCompressed byte = 0x01
	StatusZero       byte = 0x02
	StatusError      byte = 0xFF
)

// Filesystem type codes (matches main.FSType values)
const (
	FSUnknown byte = 0
	FSExt2    byte = 1
	FSExt3    byte = 2
	FSExt4    byte = 3
	FSXFS     byte = 4
	FSBtrfs   byte = 5
	FSLVM     byte = 6
	FSSwap    byte = 7
)

// Handshake: client and agent exchange [4B magic][2B version]
type Handshake struct {
	Magic   [4]byte
	Version uint16
}

func WriteHandshake(w io.Writer, h *Handshake) error {
	if _, err := w.Write(h.Magic[:]); err != nil {
		return err
	}
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], h.Version)
	_, err := w.Write(buf[:])
	return err
}

func ReadHandshake(r io.Reader) (*Handshake, error) {
	var h Handshake
	if _, err := io.ReadFull(r, h.Magic[:]); err != nil {
		return nil, err
	}
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}
	h.Version = binary.LittleEndian.Uint16(buf[:])
	return &h, nil
}

// ── Request encoding ──

// ReadReq: CmdRead [8B offset] [4B length]
type ReadReq struct {
	Offset uint64
	Length uint32
}

func WriteReadReq(w io.Writer, r *ReadReq) error {
	var buf [13]byte
	buf[0] = CmdRead
	binary.LittleEndian.PutUint64(buf[1:], r.Offset)
	binary.LittleEndian.PutUint32(buf[9:], r.Length)
	_, err := w.Write(buf[:])
	return err
}

// BitmapReq: CmdBitmap [8B part_offset] [8B part_size] [1B fs_type] [2B devPathLen] [devPath]
type BitmapReq struct {
	PartOffset uint64
	PartSize   uint64
	FSType     byte
	DevPath    string // partition device path (needed for LVM)
}

func WriteBitmapReq(w io.Writer, r *BitmapReq) error {
	pathBytes := []byte(r.DevPath)
	var buf [18]byte
	buf[0] = CmdBitmap
	binary.LittleEndian.PutUint64(buf[1:], r.PartOffset)
	binary.LittleEndian.PutUint64(buf[9:], r.PartSize)
	buf[17] = r.FSType
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	var lenBuf [2]byte
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(pathBytes)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(pathBytes)
	return err
}

// PrepareReq: CmdPrepare [2B devPathLen] [devPath]
type PrepareReq struct {
	DevPath string
}

func WritePrepareReq(w io.Writer, r *PrepareReq) error {
	pathBytes := []byte(r.DevPath)
	var buf [3]byte
	buf[0] = CmdPrepare
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(pathBytes)))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	_, err := w.Write(pathBytes)
	return err
}

// Compression modes for StreamReadReq
const (
	CompressZSTD     byte = 0 // zstd default (level 3)
	CompressZSTDFast byte = 1 // zstd fastest (level 1)
	CompressNone     byte = 255 // no compression
)

// StreamReadReq: CmdStreamRead [8B offset] [8B totalLength] [4B chunkSize] [1B compressMode]
// Agent will continuously push responses (StatusOK/StatusCompressed/StatusZero)
// until totalLength bytes have been covered. Final response has StatusOK with 0 length
// to signal end-of-stream.
type StreamReadReq struct {
	Offset       uint64
	TotalLength  uint64
	ChunkSize    uint32
	CompressMode byte
}

func WriteStreamReadReq(w io.Writer, r *StreamReadReq) error {
	var buf [22]byte
	buf[0] = CmdStreamRead
	binary.LittleEndian.PutUint64(buf[1:], r.Offset)
	binary.LittleEndian.PutUint64(buf[9:], r.TotalLength)
	binary.LittleEndian.PutUint32(buf[17:], r.ChunkSize)
	buf[21] = r.CompressMode
	_, err := w.Write(buf[:])
	return err
}

func ReadStreamReadReq(r io.Reader) (*StreamReadReq, error) {
	var buf [21]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}
	return &StreamReadReq{
		Offset:       binary.LittleEndian.Uint64(buf[0:]),
		TotalLength:  binary.LittleEndian.Uint64(buf[8:]),
		ChunkSize:    binary.LittleEndian.Uint32(buf[16:]),
		CompressMode: buf[20],
	}, nil
}

func WriteCloseReq(w io.Writer) error {
	_, err := w.Write([]byte{CmdClose})
	return err
}

// ReadCommand reads the command byte from the stream.
func ReadCommand(r io.Reader) (byte, error) {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func ReadReadReq(r io.Reader) (*ReadReq, error) {
	var buf [12]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}
	return &ReadReq{
		Offset: binary.LittleEndian.Uint64(buf[0:]),
		Length: binary.LittleEndian.Uint32(buf[8:]),
	}, nil
}

func ReadBitmapReq(r io.Reader) (*BitmapReq, error) {
	var buf [17]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}
	req := &BitmapReq{
		PartOffset: binary.LittleEndian.Uint64(buf[0:]),
		PartSize:   binary.LittleEndian.Uint64(buf[8:]),
		FSType:     buf[16],
	}
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	pathLen := binary.LittleEndian.Uint16(lenBuf[:])
	if pathLen > 0 {
		pathBuf := make([]byte, pathLen)
		if _, err := io.ReadFull(r, pathBuf); err != nil {
			return nil, err
		}
		req.DevPath = string(pathBuf)
	}
	return req, nil
}

func ReadPrepareReq(r io.Reader) (*PrepareReq, error) {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}
	pathLen := binary.LittleEndian.Uint16(buf[:])
	if pathLen > 1024 {
		return nil, fmt.Errorf("devPath too long: %d", pathLen)
	}
	pathBuf := make([]byte, pathLen)
	if _, err := io.ReadFull(r, pathBuf); err != nil {
		return nil, err
	}
	return &PrepareReq{DevPath: string(pathBuf)}, nil
}

// ── Response encoding ──
// Response: [1B status] [4B compressed_len] [4B original_len] [payload]

type ResponseHeader struct {
	Status      byte
	CompLen     uint32 // compressed payload length (0 for StatusZero/StatusError with no payload)
	OriginalLen uint32 // original (uncompressed) length; for StatusZero = bytes to skip
}

func WriteResponseHeader(w io.Writer, h *ResponseHeader) error {
	var buf [9]byte
	buf[0] = h.Status
	binary.LittleEndian.PutUint32(buf[1:], h.CompLen)
	binary.LittleEndian.PutUint32(buf[5:], h.OriginalLen)
	_, err := w.Write(buf[:])
	return err
}

func ReadResponseHeader(r io.Reader) (*ResponseHeader, error) {
	var buf [9]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}
	return &ResponseHeader{
		Status:      buf[0],
		CompLen:     binary.LittleEndian.Uint32(buf[1:]),
		OriginalLen: binary.LittleEndian.Uint32(buf[5:]),
	}, nil
}

// WriteErrorResponse sends an error response with message.
func WriteErrorResponse(w io.Writer, msg string) error {
	msgBytes := []byte(msg)
	h := &ResponseHeader{
		Status:      StatusError,
		CompLen:     uint32(len(msgBytes)),
		OriginalLen: 0,
	}
	if err := WriteResponseHeader(w, h); err != nil {
		return err
	}
	_, err := w.Write(msgBytes)
	return err
}

// WriteOKResponse sends an uncompressed OK response.
func WriteOKResponse(w io.Writer, data []byte) error {
	h := &ResponseHeader{
		Status:      StatusOK,
		CompLen:     uint32(len(data)),
		OriginalLen: uint32(len(data)),
	}
	if err := WriteResponseHeader(w, h); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// WriteCompressedResponse sends a ZSTD-compressed response.
func WriteCompressedResponse(w io.Writer, compressed []byte, originalLen uint32) error {
	h := &ResponseHeader{
		Status:      StatusCompressed,
		CompLen:     uint32(len(compressed)),
		OriginalLen: originalLen,
	}
	if err := WriteResponseHeader(w, h); err != nil {
		return err
	}
	_, err := w.Write(compressed)
	return err
}

// WriteZeroResponse sends a zero-region response (no payload).
func WriteZeroResponse(w io.Writer, length uint32) error {
	h := &ResponseHeader{
		Status:      StatusZero,
		CompLen:     0,
		OriginalLen: length,
	}
	return WriteResponseHeader(w, h)
}

// DiskInfoResponse is sent in response to CmdDiskInfo.
// Format: [8B diskSize] [2B numDisks] then per disk:
//   [2B nameLen] [name] [8B size] [2B modelLen] [model]
type DiskEntry struct {
	Name  string
	Size  uint64
	Model string
}

func WriteDiskInfoResponse(w io.Writer, disks []DiskEntry) error {
	// First build payload
	var payload []byte

	// numDisks
	var buf2 [2]byte
	binary.LittleEndian.PutUint16(buf2[:], uint16(len(disks)))
	payload = append(payload, buf2[:]...)

	for _, d := range disks {
		nameBytes := []byte(d.Name)
		binary.LittleEndian.PutUint16(buf2[:], uint16(len(nameBytes)))
		payload = append(payload, buf2[:]...)
		payload = append(payload, nameBytes...)

		var buf8 [8]byte
		binary.LittleEndian.PutUint64(buf8[:], d.Size)
		payload = append(payload, buf8[:]...)

		modelBytes := []byte(d.Model)
		binary.LittleEndian.PutUint16(buf2[:], uint16(len(modelBytes)))
		payload = append(payload, buf2[:]...)
		payload = append(payload, modelBytes...)
	}

	return WriteOKResponse(w, payload)
}

func ReadDiskInfoResponse(data []byte) ([]DiskEntry, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("diskinfo response too short")
	}
	numDisks := binary.LittleEndian.Uint16(data[0:])
	pos := 2
	var disks []DiskEntry
	for i := 0; i < int(numDisks); i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("truncated diskinfo")
		}
		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+nameLen > len(data) {
			return nil, fmt.Errorf("truncated diskinfo name")
		}
		name := string(data[pos : pos+nameLen])
		pos += nameLen

		if pos+8 > len(data) {
			return nil, fmt.Errorf("truncated diskinfo size")
		}
		size := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		if pos+2 > len(data) {
			return nil, fmt.Errorf("truncated diskinfo model")
		}
		modelLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+modelLen > len(data) {
			return nil, fmt.Errorf("truncated diskinfo model data")
		}
		model := string(data[pos : pos+modelLen])
		pos += modelLen

		disks = append(disks, DiskEntry{Name: name, Size: size, Model: model})
	}
	return disks, nil
}
