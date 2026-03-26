package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"sshimager/protocol"
)

func printUsage() {
	fmt.Fprintf(os.Stderr, `sshimager v%s — Remote disk imaging over SSH

Usage:
  sshimager <user@host> [options]              Interactive (select disk in TUI)
  sshimager <user@host>:/dev/sdX [options]     Specify disk directly

Options:
  -p <port>            SSH port (default: 22)
  -o <file>            Output file (.vmdk .vhd .vdi .dd)
  -f <format>          Force format: vmdk, vhd, vdi, dd
  -i                   Interactive mode (TUI for partition selection)
  --agent              Use agent mode (faster, ZSTD compression)
  --compress <mode>    Agent compression: zstd-fast (default), zstd, none
  --exclude <N,...>    Exclude partition numbers
  --used-only <N,...>  Used-only mode for partitions (bitmap-aware)
  --used-only-all      Used-only for all supported partitions
  --buf-size <MB>      IO buffer size (default: 4 agent, 8 sftp)

Note: Network interruptions during transfer are handled automatically.
      The tool will retry reconnecting indefinitely until the transfer completes.

Examples:
  sshimager root@192.168.1.50 -i
  sshimager root@192.168.1.50:/dev/sda -o server.vmdk -i
  sshimager root@192.168.1.50 -p 2222 -o server.vmdk --agent
  sshimager root@192.168.1.50:/dev/sda -o server.vmdk --exclude 3 --used-only 1,2
  sshimager user@host -o backup.vhd --used-only-all

`, Version)
}

func main() {
	var (
		sshPort     int
		output      string
		formatStr   string
		interactive bool
		useAgent    bool
		compressStr string
		excludeStr  string
		usedOnlyStr string
		usedOnlyAll bool
		bufMB       int
	)

	fs := flag.NewFlagSet("sshimager", flag.ExitOnError)
	fs.IntVar(&sshPort, "p", 22, "SSH port")
	fs.StringVar(&output, "o", "", "Output file")
	fs.StringVar(&formatStr, "f", "", "Force format")
	fs.BoolVar(&interactive, "i", false, "Interactive mode")
	fs.BoolVar(&useAgent, "agent", false, "Use agent mode")
	fs.StringVar(&compressStr, "compress", "zstd-fast", "Compression: zstd-fast, zstd, none")
	fs.StringVar(&excludeStr, "exclude", "", "Exclude partitions")
	fs.StringVar(&usedOnlyStr, "used-only", "", "Used-only partitions")
	fs.BoolVar(&usedOnlyAll, "used-only-all", false, "Used-only for all")
	fs.IntVar(&bufMB, "buf-size", 0, "Buffer size in MB (default: 4 agent, 8 sftp)")
	fs.Usage = printUsage

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Find target spec: user@host or user@host:/dev/xxx
	targetSpec := ""
	var remaining []string
	for _, arg := range os.Args[1:] {
		if strings.Contains(arg, "@") && !strings.HasPrefix(arg, "-") {
			targetSpec = arg
		} else {
			remaining = append(remaining, arg)
		}
	}

	if targetSpec == "" {
		if os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help" {
			printUsage()
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "Error: no remote target specified. Use user@host or user@host:/dev/sdX\n")
		os.Exit(1)
	}

	fs.Parse(remaining)

	// Parse user@host and optional :/dev/xxx
	userHost := targetSpec
	devPath := ""
	if colon := strings.LastIndex(targetSpec, ":/dev/"); colon >= 0 {
		userHost = targetSpec[:colon]
		devPath = targetSpec[colon+1:]
	}

	// If no -o and no -i, default to interactive
	if output == "" && !interactive && devPath == "" {
		interactive = true
	}

	// ── Step 1: Connect ──
	conn, err := NewSSHConn(userHost, sshPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// ── Step 2: Select disk ──
	// Disk selection always uses SSH commands (works before backend is set up)
	if devPath == "" {
		fmt.Fprintf(os.Stderr, "Discovering remote disks...\n")
		disks, err := conn.ListDisks()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listing disks: %v\n", err)
			os.Exit(1)
		}

		if len(disks) == 0 {
			fmt.Fprintf(os.Stderr, "No disks found on remote.\n")
			os.Exit(1)
		}

		if len(disks) == 1 {
			devPath = disks[0].Dev
			fmt.Fprintf(os.Stderr, "Auto-selected: %s (%s, %s)\n",
				devPath, disks[0].Model, FormatSize(disks[0].Size))
		} else {
			devPath = TUIDiskSelect(disks, userHost)
			if devPath == "" {
				fmt.Fprintf(os.Stderr, "Cancelled.\n")
				os.Exit(0)
			}
		}
		interactive = true
	}

	// ── Step 3: Create backend and prepare disk ──
	// Apply default buf-size based on backend
	if bufMB == 0 {
		if useAgent {
			bufMB = 4
		} else {
			bufMB = 8
		}
	}

	var backend DiskBackend
	if useAgent {
		fmt.Fprintf(os.Stderr, "Setting up agent backend...\n")
		ab, err := NewAgentBackend(conn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer ab.Close()

		// Set compression mode
		switch strings.ToLower(compressStr) {
		case "none":
			ab.CompressMode = protocol.CompressNone
			fmt.Fprintf(os.Stderr, "Compression: none\n")
		case "zstd-fast":
			ab.CompressMode = protocol.CompressZSTDFast
			fmt.Fprintf(os.Stderr, "Compression: zstd-fast\n")
		default:
			ab.CompressMode = protocol.CompressZSTD
			fmt.Fprintf(os.Stderr, "Compression: zstd\n")
		}

		// Agent handles prepare+open in one command
		if err := ab.PrepareDisk(devPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		backend = ab
	} else {
		// SFTP mode: set up SFTP subsystem, then backend
		if err := conn.SetupSFTP(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		sftpBackend := NewSFTPBackend(conn)
		defer sftpBackend.Close()

		conn.PrepareDisk(devPath)
		if err := conn.OpenDisk(devPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		backend = sftpBackend
	}

	// ── Step 4: Scan partitions ──
	// Partition scanning uses SSH commands + disk reads via backend
	fmt.Fprintf(os.Stderr, "Scanning partitions on %s...\n", devPath)
	diskSize, err := conn.GetDiskSize()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	disk, err := ScanPartitions(backend, diskSize, devPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	disk.Model = conn.GetDiskModel()
	conn.ResolveMounts(disk)

	fmt.Fprintf(os.Stderr, "Disk: %s  %s  %s  %d partitions\n",
		disk.DevPath, disk.Model, FormatSize(disk.Size), len(disk.Partitions))
	for _, p := range disk.Partitions {
		fmt.Fprintf(os.Stderr, "  #%d  %-20s  %-8s  %10s  %s\n",
			p.Number, p.DevPath, p.FSType, FormatSize(p.Size), p.Mountpoint)
	}

	// ── Step 5: Configure ──
	format := FormatVMDK
	if interactive {
		var ok bool
		output, format, ok = TUIPartitionConfig(disk, output)
		if !ok {
			fmt.Fprintf(os.Stderr, "Cancelled.\n")
			os.Exit(0)
		}
	} else {
		if formatStr != "" {
			format = FormatFromName(formatStr)
		} else if output != "" {
			format = FormatFromExt(output)
		}
		if excludeStr != "" {
			applyExclude(disk, excludeStr)
		}
		if usedOnlyAll {
			for i := range disk.Partitions {
				if disk.Partitions[i].FSType.SupportsBitmap() && disk.Partitions[i].CopyMode == CopyFull {
					disk.Partitions[i].CopyMode = CopyUsedOnly
				}
			}
		} else if usedOnlyStr != "" {
			applyUsedOnly(disk, usedOnlyStr)
		}
	}

	if output == "" {
		fmt.Fprintf(os.Stderr, "Error: no output file specified\n")
		os.Exit(1)
	}

	// Enable VT processing for progress output (after TUI is done)
	initTerminal()

	// ── Step 6: Image ──
	regions := BuildRegions(disk)
	cfg := &ImagingConfig{
		Backend: backend,
		Disk:    disk,
		Output:  output,
		Format:  format,
		BufSize: bufMB * 1024 * 1024,
		Regions: regions,
	}

	if err := RunImaging(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
		os.Exit(1)
	}
}

func applyExclude(disk *DiskInfo, spec string) {
	for _, s := range strings.Split(spec, ",") {
		s = strings.TrimSpace(s)
		if num, err := strconv.Atoi(s); err == nil {
			for i := range disk.Partitions {
				if disk.Partitions[i].Number == num {
					disk.Partitions[i].CopyMode = CopySkip
				}
			}
		}
	}
}

func applyUsedOnly(disk *DiskInfo, spec string) {
	for _, s := range strings.Split(spec, ",") {
		s = strings.TrimSpace(s)
		if num, err := strconv.Atoi(s); err == nil {
			for i := range disk.Partitions {
				if disk.Partitions[i].Number == num && disk.Partitions[i].FSType.SupportsBitmap() {
					disk.Partitions[i].CopyMode = CopyUsedOnly
				}
			}
		}
	}
}
