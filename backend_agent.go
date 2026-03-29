package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/ssh"
	"golang.org/x/term"

	"sshimager/bitmap"
	"sshimager/protocol"
)

const agentRemotePath = "/tmp/.sshimager-agent"

// AgentBackend implements DiskBackend using the custom agent protocol.
type AgentBackend struct {
	conn         *SSHConn
	session      *ssh.Session
	stdin        io.WriteCloser
	stdout       io.Reader
	reader       *bufio.Reader
	writer       *bufio.Writer
	zdec         *zstd.Decoder
	CompressMode byte // protocol.CompressZSTD, CompressZSTDFast, CompressNone
	pingStop     chan struct{}
	pingMu       sync.Mutex // serialize writes (ping vs commands)
}

// NewAgentBackend uploads and launches the agent on the remote host.
func NewAgentBackend(conn *SSHConn) (*AgentBackend, error) {
	// 1. Detect remote architecture
	arch, err := detectRemoteArch(conn)
	if err != nil {
		return nil, fmt.Errorf("detect arch: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Remote architecture: %s\n", arch)

	// 2. Find local agent binary
	agentPath, err := findAgentBinary(arch)
	if err != nil {
		return nil, fmt.Errorf("find agent binary: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Using agent binary: %s\n", agentPath)

	// 3. Upload agent via SSH exec (no sftp-server needed)
	if err := uploadAgent(conn, agentPath); err != nil {
		return nil, fmt.Errorf("upload agent: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Agent uploaded to %s\n", agentRemotePath)

	// 4. Launch agent
	ab, err := launchAgent(conn)
	if err != nil && !conn.isRoot {
		// Sudo password might be wrong — prompt for separate sudo password
		fmt.Fprintf(os.Stderr, "Agent launch failed (sudo password may be wrong): %v\n", err)
		fmt.Fprintf(os.Stderr, "[sudo] password for %s: ", conn.User)
		passBytes, passErr := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if passErr == nil {
			conn.sudoPass = string(passBytes)
			ab, err = launchAgent(conn)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("launch agent: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Agent connected (protocol v%d)\n", protocol.Version)
	return ab, nil
}

func detectRemoteArch(conn *SSHConn) (string, error) {
	out, err := conn.ExecCommand("uname -m")
	if err != nil {
		return "", err
	}
	arch := strings.TrimSpace(out)
	// Normalize
	switch arch {
	case "x86_64":
		return "amd64", nil
	case "aarch64":
		return "arm64", nil
	case "i686", "i386":
		return "386", nil
	case "armv7l":
		return "arm", nil
	}
	return arch, nil
}

func findAgentBinary(goarch string) (string, error) {
	// Look for agent binary relative to our own executable
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}
	exeDir := filepath.Dir(exe)

	// Search paths in order
	candidates := []string{
		filepath.Join(exeDir, "agents", "linux_"+goarch),
		filepath.Join(exeDir, "..", "agents", "linux_"+goarch),
		filepath.Join("agents", "linux_"+goarch),
	}

	// Also check working directory
	cwd, _ := os.Getwd()
	if cwd != "" {
		candidates = append(candidates,
			filepath.Join(cwd, "agents", "linux_"+goarch),
			filepath.Join(cwd, "bin", "agents", "linux_"+goarch),
		)
	}

	// If running on Linux with matching arch, the agent binary in bin/
	if runtime.GOOS == "linux" && runtime.GOARCH == goarch {
		candidates = append(candidates, filepath.Join(exeDir, "sshimager-agent"))
	}

	for _, p := range candidates {
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
			return p, nil
		}
	}
	return "", fmt.Errorf("agent binary for linux/%s not found (searched: %v)", goarch, candidates)
}

// uploadAgent uploads the agent binary via SSH exec "cat > path"
func uploadAgent(conn *SSHConn, localPath string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}

	// Use sudo if not root to write to /tmp and chmod
	cmd := fmt.Sprintf("cat > %s && chmod +x %s", agentRemotePath, agentRemotePath)
	if !conn.isRoot {
		// /tmp is usually world-writable, no sudo needed for writing there
		cmd = fmt.Sprintf("cat > %s && chmod +x %s", agentRemotePath, agentRemotePath)
	}

	session, err := conn.sshClient.NewSession()
	if err != nil {
		return err
	}

	stdin, err := session.StdinPipe()
	if err != nil {
		session.Close()
		return err
	}

	if err := session.Start(cmd); err != nil {
		session.Close()
		return fmt.Errorf("start upload cmd: %w", err)
	}

	_, err = stdin.Write(data)
	if err != nil {
		session.Close()
		return fmt.Errorf("write agent data: %w", err)
	}
	stdin.Close()

	if err := session.Wait(); err != nil {
		session.Close()
		return fmt.Errorf("upload wait: %w", err)
	}
	session.Close()
	return nil
}

func launchAgent(conn *SSHConn) (*AgentBackend, error) {
	session, err := conn.sshClient.NewSession()
	if err != nil {
		return nil, err
	}

	stdin, err := session.StdinPipe()
	if err != nil {
		session.Close()
		return nil, err
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		session.Close()
		return nil, err
	}

	// Launch with sudo if not root (need root to read disk devices)
	// Use sudo -S so password is read from stdin, then agent takes over stdin/stdout
	cmd := fmt.Sprintf("%s --serve --delete", agentRemotePath)
	if !conn.isRoot {
		cmd = fmt.Sprintf("sudo -S %s --serve --delete", agentRemotePath)
	}

	if err := session.Start(cmd); err != nil {
		session.Close()
		return nil, fmt.Errorf("start agent: %w", err)
	}

	// If non-root, write sudo password directly to stdin pipe
	// (same approach as launchSudoSFTP — keeps stdin connected to session)
	if !conn.isRoot {
		if _, err := fmt.Fprintf(stdin, "%s\n", conn.sudoPass); err != nil {
			session.Close()
			return nil, fmt.Errorf("send sudo password: %w", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	reader := bufio.NewReaderSize(stdout, 256*1024)
	writer := bufio.NewWriterSize(stdin, 256*1024)

	// Read agent handshake
	agentHS, err := protocol.ReadHandshake(reader)
	if err != nil {
		session.Close()
		return nil, fmt.Errorf("agent handshake read: %w", err)
	}
	if agentHS.Magic != protocol.Magic {
		session.Close()
		return nil, fmt.Errorf("agent bad magic: %v", agentHS.Magic)
	}

	// Send our handshake
	clientHS := &protocol.Handshake{Magic: protocol.Magic, Version: protocol.Version}
	if err := protocol.WriteHandshake(writer, clientHS); err != nil {
		session.Close()
		return nil, fmt.Errorf("client handshake write: %w", err)
	}
	writer.Flush()

	zdec, err := zstd.NewReader(nil)
	if err != nil {
		session.Close()
		return nil, fmt.Errorf("zstd decoder init: %w", err)
	}

	ab := &AgentBackend{
		conn:    conn,
		session: session,
		stdin:   stdin,
		stdout:  stdout,
		reader:  reader,
		writer:  writer,
		zdec:    zdec,
	}
	ab.startPing()
	return ab, nil
}

// PrepareDisk sends CmdPrepare to the agent
func (ab *AgentBackend) PrepareDisk(devPath string) error {
	fmt.Fprintf(os.Stderr, "Preparing disk %s (sync + drop_caches)...\n", devPath)
	// Set DevPath on conn so GetDiskSize/GetDiskModel/ResolveMounts work
	ab.conn.DevPath = devPath

	ab.pingMu.Lock()
	err := protocol.WritePrepareReq(ab.writer, &protocol.PrepareReq{DevPath: devPath})
	if err == nil {
		err = ab.writer.Flush()
	}
	ab.pingMu.Unlock()
	if err != nil {
		return err
	}

	_, err = ab.readResponsePayload()
	return err
}

func (ab *AgentBackend) ReadAt(p []byte, off int64) (int, error) {
	length := uint32(len(p))
	if length > 1024*1024 {
		length = 1024 * 1024
	}

	ab.pingMu.Lock()
	err := protocol.WriteReadReq(ab.writer, &protocol.ReadReq{
		Offset: uint64(off),
		Length: length,
	})
	if err == nil {
		err = ab.writer.Flush()
	}
	ab.pingMu.Unlock()
	if err != nil {
		return 0, err
	}

	hdr, err := protocol.ReadResponseHeader(ab.reader)
	if err != nil {
		return 0, err
	}

	switch hdr.Status {
	case protocol.StatusZero:
		// Fill with zeros
		n := int(hdr.OriginalLen)
		if n > len(p) {
			n = len(p)
		}
		for i := 0; i < n; i++ {
			p[i] = 0
		}
		return n, nil

	case protocol.StatusOK:
		// Uncompressed data
		if hdr.CompLen > uint32(len(p)) {
			return 0, fmt.Errorf("response too large: %d > %d", hdr.CompLen, len(p))
		}
		if _, err := io.ReadFull(ab.reader, p[:hdr.CompLen]); err != nil {
			return 0, err
		}
		return int(hdr.CompLen), nil

	case protocol.StatusCompressed:
		// Read compressed data
		compressed := make([]byte, hdr.CompLen)
		if _, err := io.ReadFull(ab.reader, compressed); err != nil {
			return 0, err
		}
		decompressed, err := ab.zdec.DecodeAll(compressed, p[:0])
		if err != nil {
			return 0, fmt.Errorf("zstd decompress: %w", err)
		}
		// decompressed was written into p's backing array
		return len(decompressed), nil

	case protocol.StatusError:
		msg := make([]byte, hdr.CompLen)
		io.ReadFull(ab.reader, msg)
		return 0, fmt.Errorf("agent error: %s", string(msg))

	default:
		return 0, fmt.Errorf("unknown status: 0x%02x", hdr.Status)
	}
}

func (ab *AgentBackend) GetBitmap(partOffset, partSize uint64, fsType FSType, devPath string) (*bitmap.BlockBitmap, error) {
	// Map FSType to protocol byte
	var fsByte byte
	switch fsType {
	case FSExt2:
		fsByte = protocol.FSExt2
	case FSExt3:
		fsByte = protocol.FSExt3
	case FSExt4:
		fsByte = protocol.FSExt4
	case FSXFS:
		fsByte = protocol.FSXFS
	case FSSwap:
		fsByte = protocol.FSSwap
	case FSLVM:
		fsByte = protocol.FSLVM
	case FSFat32:
		fsByte = protocol.FSFat32
	case FSNTFS:
		fsByte = protocol.FSNTFS
	case FSFat16:
		fsByte = protocol.FSFat16
	default:
		return nil, fmt.Errorf("agent bitmap: unsupported fs %s", fsType)
	}

	ab.pingMu.Lock()
	err := protocol.WriteBitmapReq(ab.writer, &protocol.BitmapReq{
		PartOffset: partOffset,
		PartSize:   partSize,
		FSType:     fsByte,
		DevPath:    devPath,
	})
	if err == nil {
		err = ab.writer.Flush()
	}
	ab.pingMu.Unlock()
	if err != nil {
		return nil, err
	}

	data, err := ab.readResponsePayload()
	if err != nil {
		return nil, err
	}

	// Parse: [4B blockSize] [8B totalBlocks] [bitmap bits...]
	if len(data) < 12 {
		return nil, fmt.Errorf("bitmap response too short: %d", len(data))
	}
	blockSize := binary.LittleEndian.Uint32(data[0:4])
	totalBlocks := binary.LittleEndian.Uint64(data[4:12])
	bits := data[12:]

	return &bitmap.BlockBitmap{
		Bits:        bits,
		BlockSize:   blockSize,
		TotalBlocks: totalBlocks,
	}, nil
}

// StreamCopyRegion sends a single CmdStreamRead and reads the pushed response stream,
// writing each chunk to vw. Returns the number of bytes actually received (for resume).
func (ab *AgentBackend) StreamCopyRegion(vw VDiskWriter, offset, length uint64, chunkSize uint32,
	prog *Progress, tStart time.Time) error {

	ab.pingMu.Lock()
	err := protocol.WriteStreamReadReq(ab.writer, &protocol.StreamReadReq{
		Offset:       offset,
		TotalLength:  length,
		ChunkSize:    chunkSize,
		CompressMode: ab.CompressMode,
	})
	if err == nil {
		err = ab.writer.Flush()
	}
	ab.pingMu.Unlock()
	if err != nil {
		return err
	}

	curOff := offset

	for {
		hdr, err := protocol.ReadResponseHeader(ab.reader)
		if err != nil {
			return fmt.Errorf("stream read header at %d: %w", curOff, err)
		}

		switch hdr.Status {
		case protocol.StatusOK:
			if hdr.CompLen == 0 && hdr.OriginalLen == 0 {
				// End-of-stream marker
				return nil
			}
			data := make([]byte, hdr.CompLen)
			if _, err := io.ReadFull(ab.reader, data); err != nil {
				return fmt.Errorf("stream read data at %d: %w", curOff, err)
			}
			if err := vw.Write(curOff, data); err != nil {
				return fmt.Errorf("write at %d: %w", curOff, err)
			}
			n := uint64(hdr.OriginalLen)
			curOff += n
			prog.TotalDone += n
			prog.DataWritten += n
			printProgress(prog.TotalDone, prog.TotalWork, prog.DataWritten, tStart)

		case protocol.StatusCompressed:
			compressed := make([]byte, hdr.CompLen)
			if _, err := io.ReadFull(ab.reader, compressed); err != nil {
				return fmt.Errorf("stream read compressed at %d: %w", curOff, err)
			}
			decompressed, err := ab.zdec.DecodeAll(compressed, nil)
			if err != nil {
				return fmt.Errorf("zstd decompress at %d: %w", curOff, err)
			}
			if err := vw.Write(curOff, decompressed); err != nil {
				return fmt.Errorf("write at %d: %w", curOff, err)
			}
			n := uint64(hdr.OriginalLen)
			curOff += n
			prog.TotalDone += n
			prog.DataWritten += n
			printProgress(prog.TotalDone, prog.TotalWork, prog.DataWritten, tStart)

		case protocol.StatusZero:
			// Zero region — sparse skip
			n := uint64(hdr.OriginalLen)
			if err := vw.WriteZero(curOff, n); err != nil {
				return fmt.Errorf("write zero at %d: %w", curOff, err)
			}
			curOff += n
			prog.TotalDone += n
			prog.DataWritten += n
			printProgress(prog.TotalDone, prog.TotalWork, prog.DataWritten, tStart)

		case protocol.StatusError:
			msg := make([]byte, hdr.CompLen)
			io.ReadFull(ab.reader, msg)
			return fmt.Errorf("agent stream error: %s", string(msg))

		default:
			return fmt.Errorf("unknown stream status: 0x%02x", hdr.Status)
		}
	}
}

func (ab *AgentBackend) Reconnect() error {
	fmt.Fprintf(os.Stderr, "\nReconnecting (Agent backend)...\n")
	ab.closeInternal()

	// Reconnect SSH
	if err := ab.conn.Reconnect(); err != nil {
		return err
	}

	// Re-launch agent (binary was self-deleted, need to re-upload)
	// Old agents will self-exit after 60s idle timeout.
	arch, err := detectRemoteArch(ab.conn)
	if err != nil {
		return fmt.Errorf("detect arch on reconnect: %w", err)
	}
	agentPath, err := findAgentBinary(arch)
	if err != nil {
		return fmt.Errorf("find agent on reconnect: %w", err)
	}
	if err := uploadAgent(ab.conn, agentPath); err != nil {
		return fmt.Errorf("upload agent on reconnect: %w", err)
	}

	newAb, err := launchAgent(ab.conn)
	if err != nil {
		return fmt.Errorf("launch agent on reconnect: %w", err)
	}

	// Swap internals — stop newAb's ping first, we'll start our own
	newAb.stopPing()
	ab.session = newAb.session
	ab.stdin = newAb.stdin
	ab.stdout = newAb.stdout
	ab.reader = newAb.reader
	ab.writer = newAb.writer
	ab.zdec = newAb.zdec
	ab.startPing()

	// Re-prepare disk
	if ab.conn.DevPath != "" {
		if err := ab.PrepareDisk(ab.conn.DevPath); err != nil {
			return fmt.Errorf("prepare disk on reconnect: %w", err)
		}
	}

	fmt.Fprintf(os.Stderr, "Agent reconnected.\n")
	return nil
}

func (ab *AgentBackend) IsNetworkError(err error) bool {
	return IsNetworkError(err)
}

func (ab *AgentBackend) RemoteInfo() (user, host string) {
	return ab.conn.User, ab.conn.Host
}

func (ab *AgentBackend) Close() {
	ab.closeInternal()
}

func (ab *AgentBackend) closeInternal() {
	ab.stopPing()
	if ab.writer != nil {
		ab.pingMu.Lock()
		protocol.WriteCloseReq(ab.writer)
		ab.writer.Flush()
		ab.pingMu.Unlock()
	}
	if ab.zdec != nil {
		ab.zdec.Close()
		ab.zdec = nil
	}
	if ab.session != nil {
		ab.session.Close()
		ab.session = nil
	}
}

// startPing launches a background goroutine that sends CmdPing every 30s
// to keep the agent's idle watchdog alive.
func (ab *AgentBackend) startPing() {
	ab.pingStop = make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ab.pingMu.Lock()
				ab.writer.Write([]byte{protocol.CmdPing})
				ab.writer.Flush()
				ab.pingMu.Unlock()
			case <-ab.pingStop:
				return
			}
		}
	}()
}

func (ab *AgentBackend) stopPing() {
	if ab.pingStop != nil {
		close(ab.pingStop)
		ab.pingStop = nil
	}
}

// Conn returns the underlying SSHConn for pre-imaging setup.
func (ab *AgentBackend) Conn() *SSHConn {
	return ab.conn
}

// readResponsePayload reads a full response and returns decompressed payload.
func (ab *AgentBackend) readResponsePayload() ([]byte, error) {
	hdr, err := protocol.ReadResponseHeader(ab.reader)
	if err != nil {
		return nil, err
	}

	switch hdr.Status {
	case protocol.StatusOK:
		data := make([]byte, hdr.CompLen)
		if _, err := io.ReadFull(ab.reader, data); err != nil {
			return nil, err
		}
		return data, nil

	case protocol.StatusCompressed:
		compressed := make([]byte, hdr.CompLen)
		if _, err := io.ReadFull(ab.reader, compressed); err != nil {
			return nil, err
		}
		decompressed, err := ab.zdec.DecodeAll(compressed, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd decompress: %w", err)
		}
		return decompressed, nil

	case protocol.StatusZero:
		return make([]byte, hdr.OriginalLen), nil

	case protocol.StatusError:
		msg := make([]byte, hdr.CompLen)
		io.ReadFull(ab.reader, msg)
		return nil, fmt.Errorf("agent error: %s", string(msg))

	default:
		return nil, fmt.Errorf("unknown status: 0x%02x", hdr.Status)
	}
}
