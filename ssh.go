package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/term"

	"github.com/pkg/sftp"
)

// SSHConn manages the SSH connection and provides SFTP access to remote disk
type SSHConn struct {
	Host    string // hostname:port
	User    string
	DevPath string // /dev/sda

	sshClient  *ssh.Client
	sftpClient *sftp.Client
	diskFile   *sftp.File

	// Credentials for reconnect
	password   string
	sudoPass   string
	authMethod ssh.AuthMethod
	isRoot     bool

	// sudo sftp-server session
	sudoSession *ssh.Session
	sudoStdin   io.WriteCloser
	sudoStdout  io.Reader
}

// NewSSHConn creates and connects to the remote host (without opening a disk).
func NewSSHConn(userHost string, port int) (*SSHConn, error) {
	// Parse user@host
	user := "root"
	host := userHost
	if at := strings.Index(userHost, "@"); at >= 0 {
		user = userHost[:at]
		host = userHost[at+1:]
	}
	host = fmt.Sprintf("%s:%d", host, port)

	conn := &SSHConn{
		Host: host,
		User: user,
	}

	// Get credentials
	if err := conn.getCredentials(); err != nil {
		return nil, err
	}

	// Connect
	if err := conn.connect(); err != nil {
		return nil, err
	}

	return conn, nil
}

func (c *SSHConn) getCredentials() error {
	// Try SSH key auth first (from ssh-agent or default key files)
	// For simplicity, use password auth with prompt
	fmt.Fprintf(os.Stderr, "SSH password for %s@%s: ", c.User, c.Host)
	passBytes, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return fmt.Errorf("cannot read password: %w", err)
	}
	c.password = string(passBytes)
	c.authMethod = ssh.Password(c.password)
	return nil
}

func (c *SSHConn) connect() error {
	config := &ssh.ClientConfig{
		User:            c.User,
		Auth:            []ssh.AuthMethod{c.authMethod},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         15 * time.Second,
	}

	fmt.Fprintf(os.Stderr, "Connecting to %s@%s ...\n", c.User, c.Host)

	client, err := ssh.Dial("tcp", c.Host, config)
	if err != nil {
		return fmt.Errorf("SSH connection failed: %w", err)
	}
	c.sshClient = client

	// Check if we're root
	c.isRoot, _ = c.checkRoot()

	// Default sudo password to SSH password (most common case)
	if !c.isRoot && c.sudoPass == "" {
		c.sudoPass = c.password
	}

	fmt.Fprintf(os.Stderr, "Connected to %s@%s\n", c.User, c.Host)
	return nil
}

// SetupSFTP initializes the SFTP subsystem. Only needed for SFTP backend mode.
func (c *SSHConn) SetupSFTP() error {
	if !c.isRoot {
		fmt.Fprintf(os.Stderr, "Not root. Setting up sudo sftp-server...\n")
		return c.setupSudoSFTP()
	}
	// Standard SFTP subsystem with large packet size for throughput
	sftpClient, err := sftp.NewClient(c.sshClient,
		sftp.MaxPacketChecked(32*1024),
		sftp.MaxConcurrentRequestsPerFile(64),
	)
	if err != nil {
		return fmt.Errorf("SFTP init failed: %w", err)
	}
	c.sftpClient = sftpClient
	return nil
}

func (c *SSHConn) checkRoot() (bool, error) {
	session, err := c.sshClient.NewSession()
	if err != nil {
		return false, err
	}
	defer session.Close()

	out, err := session.Output("id -u")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(out)) == "0", nil
}

func (c *SSHConn) setupSudoSFTP() error {
	// Find sftp-server path on remote first
	sftpPath, err := c.findSFTPServer()
	if err != nil {
		return fmt.Errorf("cannot find sftp-server on remote: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Found sftp-server: %s\n", sftpPath)

	// Try SSH password as sudo password first (most common case)
	c.sudoPass = c.password
	err = c.launchSudoSFTP(sftpPath)
	if err == nil {
		return nil
	}

	// SSH password didn't work for sudo — ask for separate sudo password
	fmt.Fprintf(os.Stderr, "SSH password did not work for sudo.\n")
	fmt.Fprintf(os.Stderr, "[sudo] password for %s: ", c.User)
	passBytes, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return fmt.Errorf("cannot read sudo password: %w", err)
	}
	c.sudoPass = string(passBytes)

	return c.launchSudoSFTP(sftpPath)
}

func (c *SSHConn) findSFTPServer() (string, error) {
	// Common paths
	paths := []string{
		"/usr/libexec/openssh/sftp-server", // RHEL/CentOS
		"/usr/lib/openssh/sftp-server",     // Debian/Ubuntu
		"/usr/lib/ssh/sftp-server",         // Arch
		"/usr/libexec/sftp-server",         // FreeBSD
	}

	session, err := c.sshClient.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	// Try each known path
	cmd := "for p in"
	for _, p := range paths {
		cmd += " " + p
	}
	cmd += "; do [ -x \"$p\" ] && echo \"$p\" && exit 0; done; exit 1"

	out, err := session.Output(cmd)
	if err == nil && len(out) > 0 {
		return strings.TrimSpace(string(out)), nil
	}

	// Fallback: parse sshd_config
	session2, err := c.sshClient.NewSession()
	if err != nil {
		return "", err
	}
	defer session2.Close()

	out, err = session2.Output("grep -i 'Subsystem.*sftp' /etc/ssh/sshd_config 2>/dev/null | awk '{print $NF}'")
	if err == nil && len(out) > 0 {
		p := strings.TrimSpace(string(out))
		if len(p) > 0 && p[0] == '/' {
			return p, nil
		}
	}

	return "", fmt.Errorf("sftp-server not found")
}

func (c *SSHConn) launchSudoSFTP(sftpPath string) error {
	session, err := c.sshClient.NewSession()
	if err != nil {
		return err
	}

	stdin, err := session.StdinPipe()
	if err != nil {
		session.Close()
		return err
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		session.Close()
		return err
	}

	// Start sudo sftp-server: sudo -S reads password from stdin,
	// then sftp-server takes over stdin/stdout for SFTP protocol
	cmd := fmt.Sprintf("sudo -S %s", sftpPath)
	if err := session.Start(cmd); err != nil {
		session.Close()
		return fmt.Errorf("cannot start sudo sftp-server: %w", err)
	}

	// Write sudo password + newline
	if _, err := fmt.Fprintf(stdin, "%s\n", c.sudoPass); err != nil {
		session.Close()
		return fmt.Errorf("cannot send sudo password: %w", err)
	}

	// Small delay to let sudo consume password and start sftp-server
	time.Sleep(500 * time.Millisecond)

	// Create SFTP client on this pipe with large packet for throughput
	sftpClient, err := sftp.NewClientPipe(stdout, stdin,
		sftp.MaxPacketChecked(32*1024),
		sftp.MaxConcurrentRequestsPerFile(64),
	)
	if err != nil {
		session.Close()
		return fmt.Errorf("SFTP over sudo failed: %w", err)
	}

	c.sudoSession = session
	c.sudoStdin = stdin
	c.sudoStdout = stdout
	c.sftpClient = sftpClient

	return nil
}

// PrepareDisk flushes OS caches and device buffers before imaging.
// Runs: sync && blockdev --flushbufs <dev>; sync
func (c *SSHConn) PrepareDisk(devPath string) error {
	fmt.Fprintf(os.Stderr, "Preparing disk %s (sync + flushbufs)...\n", devPath)
	cmd := fmt.Sprintf("sh -c 'sync && blockdev --flushbufs %s; sync'", devPath)
	_, err := c.ExecCommandSudo(cmd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: disk prepare failed: %v (continuing anyway)\n", err)
	}
	return nil
}

// OpenDisk opens a remote disk device via SFTP for reading.
func (c *SSHConn) OpenDisk(devPath string) error {
	// Close previous disk if any
	if c.diskFile != nil {
		c.diskFile.Close()
		c.diskFile = nil
	}
	c.DevPath = devPath
	f, err := c.sftpClient.Open(c.DevPath)
	if err != nil {
		return fmt.Errorf("cannot open remote %s: %w", c.DevPath, err)
	}
	c.diskFile = f
	return nil
}

// RemoteDisk describes a disk found on the remote system
type RemoteDisk struct {
	Name  string // e.g. "sda", "nvme0n1"
	Dev   string // e.g. "/dev/sda"
	Size  uint64
	Model string
}

// ListDisks discovers disk devices on the remote system
func (c *SSHConn) ListDisks() ([]RemoteDisk, error) {
	// List block devices from /sys/block, filter out partitions/loop/dm/ram/sr
	session, err := c.sshClient.NewSession()
	if err != nil {
		return nil, err
	}
	out, err := session.Output("ls /sys/block/ 2>/dev/null")
	session.Close()
	if err != nil {
		return nil, fmt.Errorf("cannot list /sys/block: %w", err)
	}

	var disks []RemoteDisk
	for _, name := range strings.Fields(string(out)) {
		// Skip non-disk devices
		if strings.HasPrefix(name, "loop") ||
			strings.HasPrefix(name, "dm-") ||
			strings.HasPrefix(name, "ram") ||
			strings.HasPrefix(name, "sr") ||
			strings.HasPrefix(name, "fd") ||
			strings.HasPrefix(name, "zram") {
			continue
		}

		dev := "/dev/" + name
		d := RemoteDisk{Name: name, Dev: dev}

		// Get size
		s, _ := c.ExecCommand(fmt.Sprintf("cat /sys/block/%s/size 2>/dev/null", name))
		var sectors uint64
		fmt.Sscanf(strings.TrimSpace(s), "%d", &sectors)
		d.Size = sectors * 512
		if d.Size == 0 {
			continue // skip zero-size devices
		}

		// Get model
		m, _ := c.ExecCommand(fmt.Sprintf("cat /sys/block/%s/device/model 2>/dev/null", name))
		d.Model = strings.TrimSpace(m)

		disks = append(disks, d)
	}

	return disks, nil
}

// ReadAt implements io.ReaderAt on the remote disk device
func (c *SSHConn) ReadAt(p []byte, off int64) (int, error) {
	return c.diskFile.ReadAt(p, off)
}

// GetDiskSize returns the size of the remote disk
func (c *SSHConn) GetDiskSize() (uint64, error) {
	// Method 1: SFTP stat (may not work for device files)
	// Method 2: seek to end
	// Method 3: exec command

	// Try /sys/block/xxx/size
	devBase := c.DevPath
	if idx := strings.LastIndex(devBase, "/"); idx >= 0 {
		devBase = devBase[idx+1:]
	}

	session, err := c.sshClient.NewSession()
	if err == nil {
		out, err := session.Output(fmt.Sprintf("cat /sys/block/%s/size 2>/dev/null", devBase))
		session.Close()
		if err == nil && len(out) > 0 {
			var sectors uint64
			if _, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &sectors); err == nil && sectors > 0 {
				return sectors * 512, nil
			}
		}
	}

	// Try blockdev --getsize64 (may need sudo)
	cmd := fmt.Sprintf("blockdev --getsize64 %s", c.DevPath)
	if !c.isRoot {
		cmd = fmt.Sprintf("echo '%s' | sudo -S blockdev --getsize64 %s 2>/dev/null", c.sudoPass, c.DevPath)
	}
	session2, err := c.sshClient.NewSession()
	if err == nil {
		out, err := session2.Output(cmd)
		session2.Close()
		if err == nil {
			var size uint64
			if _, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &size); err == nil && size > 0 {
				return size, nil
			}
		}
	}

	return 0, fmt.Errorf("cannot determine disk size")
}

// GetDiskModel returns disk model string
func (c *SSHConn) GetDiskModel() string {
	devBase := c.DevPath
	if idx := strings.LastIndex(devBase, "/"); idx >= 0 {
		devBase = devBase[idx+1:]
	}
	session, err := c.sshClient.NewSession()
	if err != nil {
		return ""
	}
	defer session.Close()

	out, err := session.Output(fmt.Sprintf("cat /sys/block/%s/device/model 2>/dev/null", devBase))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// ExecCommand runs a command on the remote host (for mount/lvm info)
func (c *SSHConn) ExecCommand(cmd string) (string, error) {
	session, err := c.sshClient.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	out, err := session.Output(cmd)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// ExecCommandSudo runs a command with sudo
func (c *SSHConn) ExecCommandSudo(cmd string) (string, error) {
	if c.isRoot {
		return c.ExecCommand(cmd)
	}
	return c.ExecCommand(fmt.Sprintf("echo '%s' | sudo -S %s 2>/dev/null", c.sudoPass, cmd))
}

// Reconnect re-establishes the SSH connection using saved credentials.
// Only reconnects SSH itself — SFTP and disk reopen are handled by each backend.
func (c *SSHConn) Reconnect() error {
	fmt.Fprintf(os.Stderr, "\nReconnecting SSH...\n")
	c.closeInternal()

	// Retry with backoff
	delays := []time.Duration{1, 2, 5, 10, 20, 30}
	for i, delay := range delays {
		time.Sleep(delay * time.Second)
		fmt.Fprintf(os.Stderr, "Reconnect attempt %d/%d...\n", i+1, len(delays))

		if err := c.connect(); err != nil {
			if i < len(delays)-1 {
				fmt.Fprintf(os.Stderr, "Failed: %v, retrying in %ds...\n", err, delays[i+1])
				continue
			}
			return fmt.Errorf("reconnect failed after %d attempts: %w", len(delays), err)
		}
		fmt.Fprintf(os.Stderr, "SSH reconnected.\n")
		return nil
	}
	return fmt.Errorf("reconnect exhausted")
}

// IsNetworkError checks if an error is likely a network disconnection.
// Uses errors.Is/As to handle wrapped errors from streaming protocol.
func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "connection reset") ||
		strings.Contains(s, "connection lost") ||
		strings.Contains(s, "connection closed") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "connection refused") ||
		strings.Contains(s, "use of closed") ||
		strings.Contains(s, "timeout") ||
		strings.Contains(s, "forcibly closed") ||
		strings.Contains(s, "SSH") ||
		strings.Contains(s, "sftp")
}

func (c *SSHConn) closeInternal() {
	if c.diskFile != nil {
		c.diskFile.Close()
		c.diskFile = nil
	}
	if c.sftpClient != nil {
		c.sftpClient.Close()
		c.sftpClient = nil
	}
	if c.sudoSession != nil {
		c.sudoSession.Close()
		c.sudoSession = nil
	}
	if c.sshClient != nil {
		c.sshClient.Close()
		c.sshClient = nil
	}
}

// Close closes all connections
func (c *SSHConn) Close() {
	c.closeInternal()
}

// ResolveMounts queries /proc/mounts and /proc/swaps on remote
func (c *SSHConn) ResolveMounts(info *DiskInfo) {
	mounts, _ := c.ExecCommand("cat /proc/mounts")
	swaps, _ := c.ExecCommand("cat /proc/swaps 2>/dev/null")

	for i := range info.Partitions {
		p := &info.Partitions[i]
		for _, line := range strings.Split(mounts, "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == p.DevPath {
				p.Mountpoint = fields[1]
				break
			}
		}
		if p.Mountpoint == "" && strings.Contains(swaps, p.DevPath) {
			p.Mountpoint = "[SWAP]"
		}
	}

	// LVM: resolve dm device mounts
	dmTable, err := c.ExecCommandSudo("dmsetup table 2>/dev/null")
	if err != nil || dmTable == "" {
		return
	}
	for i := range info.Partitions {
		p := &info.Partitions[i]
		if p.FSType != FSLVM || p.Mountpoint != "" {
			continue
		}
		// Get major:minor of partition device
		statOut, err := c.ExecCommand(fmt.Sprintf("stat -c '%%t:%%T' %s 2>/dev/null", p.DevPath))
		if err != nil {
			continue
		}
		var pvMaj, pvMin uint32
		fmt.Sscanf(strings.TrimSpace(statOut), "%x:%x", &pvMaj, &pvMin)
		if pvMaj == 0 && pvMin == 0 {
			continue
		}

		var lvMounts []string
		for _, line := range strings.Split(dmTable, "\n") {
			colon := strings.Index(line, ":")
			if colon < 0 {
				continue
			}
			dmName := strings.TrimSpace(line[:colon])
			var d1, d2 uint64
			var mtype string
			var dMaj, dMin uint32
			var d3 uint64
			if _, err := fmt.Sscanf(strings.TrimSpace(line[colon+1:]),
				"%d %d %s %d:%d %d", &d1, &d2, &mtype, &dMaj, &dMin, &d3); err != nil {
				continue
			}
			if mtype != "linear" || dMaj != pvMaj || dMin != pvMin {
				continue
			}
			lvDev := "/dev/mapper/" + dmName
			for _, ml := range strings.Split(mounts, "\n") {
				fields := strings.Fields(ml)
				if len(fields) >= 2 && fields[0] == lvDev {
					lvMounts = append(lvMounts, fields[1])
					break
				}
			}
			if strings.Contains(swaps, lvDev) {
				lvMounts = append(lvMounts, "[SWAP]")
			}
		}
		if len(lvMounts) > 0 {
			p.Mountpoint = strings.Join(lvMounts, ",")
		}
	}
}
