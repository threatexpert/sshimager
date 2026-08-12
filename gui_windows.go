//go:build windows && !cli

package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	"github.com/wailsapp/wails/v2/pkg/options/windows"
	wailsruntime "github.com/wailsapp/wails/v2/pkg/runtime"

	"sshimager/protocol"
)

//go:embed all:frontend/dist
var guiAssets embed.FS

type ConnectRequest struct {
	Host         string `json:"host"`
	Port         int    `json:"port"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	SudoPassword string `json:"sudoPassword"`
}

type RemoteDiskDTO struct {
	Name      string `json:"name"`
	Device    string `json:"device"`
	Model     string `json:"model"`
	Size      uint64 `json:"size"`
	SizeLabel string `json:"sizeLabel"`
}

type ConnectionResult struct {
	Remote string          `json:"remote"`
	IsRoot bool            `json:"isRoot"`
	Disks  []RemoteDiskDTO `json:"disks"`
}

type ScanDiskRequest struct {
	Device      string `json:"device"`
	UseAgent    bool   `json:"useAgent"`
	Compression string `json:"compression"`
}

type PartitionDTO struct {
	Number           int    `json:"number"`
	Device           string `json:"device"`
	FileSystem       string `json:"fileSystem"`
	Label            string `json:"label"`
	Mountpoint       string `json:"mountpoint"`
	Size             uint64 `json:"size"`
	SizeLabel        string `json:"sizeLabel"`
	CopyMode         string `json:"copyMode"`
	SupportsUsedOnly bool   `json:"supportsUsedOnly"`
}

type DiskDTO struct {
	Device     string         `json:"device"`
	Model      string         `json:"model"`
	Size       uint64         `json:"size"`
	SizeLabel  string         `json:"sizeLabel"`
	TableType  string         `json:"tableType"`
	Partitions []PartitionDTO `json:"partitions"`
}

type PartitionModeRequest struct {
	Number int    `json:"number"`
	Mode   string `json:"mode"`
}

type StartBackupRequest struct {
	Output     string                 `json:"output"`
	Format     string                 `json:"format"`
	BufferMB   int                    `json:"bufferMB"`
	Overwrite  bool                   `json:"overwrite"`
	Partitions []PartitionModeRequest `json:"partitions"`
}

type BackupDoneEvent struct {
	Success   bool   `json:"success"`
	Cancelled bool   `json:"cancelled"`
	Output    string `json:"output"`
	Error     string `json:"error,omitempty"`
}

// GUIApp owns one remote session and one imaging task at a time.
type GUIApp struct {
	ctx context.Context

	mu            sync.Mutex
	operationMu   sync.Mutex
	conn          *SSHConn
	backend       DiskBackend
	disk          *DiskInfo
	useAgent      bool
	backupRunning bool
	backupCancel  context.CancelFunc
	taskbar       *taskbarProgress
}

func NewGUIApp() *GUIApp {
	return &GUIApp{}
}

func (a *GUIApp) startup(ctx context.Context) {
	a.ctx = ctx
	a.taskbar = newTaskbarProgress()
}

func (a *GUIApp) shutdown(context.Context) {
	a.mu.Lock()
	cancel := a.backupCancel
	backend := a.backend
	conn := a.conn
	a.backupCancel = nil
	a.backend = nil
	a.conn = nil
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if backend != nil {
		backend.Close()
	}
	if conn != nil {
		conn.Close()
	}
	a.taskbar.close()
}

func (a *GUIApp) GetVersion() string {
	return Version
}

func (a *GUIApp) OpenProjectPage() {
	wailsruntime.BrowserOpenURL(a.ctx, "https://github.com/threatexpert/sshimager")
}

// Disconnect closes the complete remote session and resets scanned state.
func (a *GUIApp) Disconnect() error {
	a.operationMu.Lock()
	defer a.operationMu.Unlock()

	a.mu.Lock()
	if a.backupRunning {
		a.mu.Unlock()
		return fmt.Errorf("备份任务运行期间不能断开连接，请先取消任务")
	}
	backend := a.backend
	conn := a.conn
	a.backend = nil
	a.conn = nil
	a.disk = nil
	a.useAgent = false
	a.mu.Unlock()

	// Every backend owns and closes its SSHConn. A connection without a
	// prepared backend must be closed directly.
	if backend != nil {
		backend.Close()
	} else if conn != nil {
		conn.Close()
	}
	return nil
}

func (a *GUIApp) Connect(request ConnectRequest) (ConnectionResult, error) {
	a.operationMu.Lock()
	defer a.operationMu.Unlock()

	host := strings.TrimSpace(request.Host)
	username := strings.TrimSpace(request.Username)
	if host == "" {
		return ConnectionResult{}, fmt.Errorf("请输入远程主机地址")
	}
	if username == "" {
		username = "root"
	}
	if request.Port <= 0 || request.Port > 65535 {
		return ConnectionResult{}, fmt.Errorf("SSH 端口必须在 1 到 65535 之间")
	}

	a.mu.Lock()
	if a.backupRunning {
		a.mu.Unlock()
		return ConnectionResult{}, fmt.Errorf("备份任务运行期间不能切换连接")
	}
	oldBackend := a.backend
	oldConn := a.conn
	a.backend = nil
	a.conn = nil
	a.disk = nil
	a.mu.Unlock()
	if oldBackend != nil {
		oldBackend.Close()
	}
	if oldConn != nil {
		oldConn.Close()
	}

	userHost := username + "@" + host
	conn, err := NewSSHConnWithCredentials(userHost, request.Port, request.Password, request.SudoPassword)
	if err != nil {
		return ConnectionResult{}, err
	}
	disks, err := conn.ListDisks()
	if err != nil {
		conn.Close()
		return ConnectionResult{}, fmt.Errorf("读取远程磁盘列表失败：%w", err)
	}
	if len(disks) == 0 {
		conn.Close()
		return ConnectionResult{}, fmt.Errorf("远程主机没有可备份的物理磁盘")
	}

	result := ConnectionResult{
		Remote: username + "@" + host,
		IsRoot: conn.isRoot,
		Disks:  make([]RemoteDiskDTO, 0, len(disks)),
	}
	for _, disk := range disks {
		result.Disks = append(result.Disks, RemoteDiskDTO{
			Name: disk.Name, Device: disk.Dev, Model: disk.Model,
			Size: disk.Size, SizeLabel: FormatSize(disk.Size),
		})
	}

	a.mu.Lock()
	a.conn = conn
	a.mu.Unlock()
	return result, nil
}

func (a *GUIApp) ScanDisk(request ScanDiskRequest) (DiskDTO, error) {
	a.operationMu.Lock()
	defer a.operationMu.Unlock()

	device := strings.TrimSpace(request.Device)
	if device == "" || !strings.HasPrefix(device, "/dev/") {
		return DiskDTO{}, fmt.Errorf("请选择有效的远程磁盘")
	}

	a.mu.Lock()
	if a.backupRunning {
		a.mu.Unlock()
		return DiskDTO{}, fmt.Errorf("备份任务正在运行")
	}
	conn := a.conn
	oldBackend := a.backend
	a.backend = nil
	a.disk = nil
	a.mu.Unlock()
	if conn == nil {
		return DiskDTO{}, fmt.Errorf("SSH 会话已断开，请重新连接")
	}

	if oldBackend != nil {
		oldBackend.Close()
		conn.Close()
		if err := conn.connect(); err != nil {
			return DiskDTO{}, fmt.Errorf("重新初始化连接失败：%w", err)
		}
	}

	backend, err := createGUIBackend(conn, request)
	if err != nil {
		return DiskDTO{}, err
	}

	diskSize, err := conn.GetDiskSize()
	if err != nil {
		backend.Close()
		return DiskDTO{}, err
	}
	disk, err := ScanPartitions(backend, diskSize, device)
	if err != nil {
		backend.Close()
		return DiskDTO{}, fmt.Errorf("扫描分区失败：%w", err)
	}
	disk.Model = conn.GetDiskModel()
	conn.ResolveMounts(disk)

	a.mu.Lock()
	a.backend = backend
	a.disk = disk
	a.useAgent = request.UseAgent
	a.mu.Unlock()
	return diskToDTO(disk), nil
}

func createGUIBackend(conn *SSHConn, request ScanDiskRequest) (DiskBackend, error) {
	if request.UseAgent {
		backend, err := NewAgentBackend(conn)
		if err != nil {
			return nil, fmt.Errorf("启动高速 Agent 失败：%w", err)
		}
		switch strings.ToLower(request.Compression) {
		case "none":
			backend.CompressMode = protocol.CompressNone
		case "zstd":
			backend.CompressMode = protocol.CompressZSTD
		default:
			backend.CompressMode = protocol.CompressZSTDFast
		}
		if err := backend.PrepareDisk(request.Device); err != nil {
			backend.Close()
			return nil, err
		}
		return backend, nil
	}

	if err := conn.SetupSFTP(); err != nil {
		return nil, err
	}
	conn.PrepareDisk(request.Device)
	if err := conn.OpenDisk(request.Device); err != nil {
		return nil, err
	}
	return NewSFTPBackend(conn), nil
}

func diskToDTO(disk *DiskInfo) DiskDTO {
	tableType := "无分区表"
	if disk.PTType == PTGPT {
		tableType = "GPT"
	} else if disk.PTType == PTMBR {
		tableType = "MBR"
	}
	result := DiskDTO{
		Device: disk.DevPath, Model: disk.Model, Size: disk.Size,
		SizeLabel: FormatSize(disk.Size), TableType: tableType,
		Partitions: make([]PartitionDTO, 0, len(disk.Partitions)),
	}
	for _, partition := range disk.Partitions {
		result.Partitions = append(result.Partitions, PartitionDTO{
			Number: partition.Number, Device: partition.DevPath,
			FileSystem: partition.FSType.String(), Label: partition.FSLabel,
			Mountpoint: partition.Mountpoint, Size: partition.Size,
			SizeLabel: FormatSize(partition.Size), CopyMode: "full",
			SupportsUsedOnly: partition.FSType.SupportsBitmap(),
		})
	}
	return result
}

func (a *GUIApp) SelectOutputPath(format, suggestedName string) (string, error) {
	format = strings.ToLower(format)
	ext := ".vmdk"
	filter := "VMware 磁盘 (*.vmdk)"
	switch format {
	case "vhd":
		ext, filter = ".vhd", "Hyper-V 磁盘 (*.vhd)"
	case "vdi":
		ext, filter = ".vdi", "VirtualBox 磁盘 (*.vdi)"
	case "dd", "raw":
		ext, filter = ".dd", "原始磁盘镜像 (*.dd)"
	}
	if strings.TrimSpace(suggestedName) == "" {
		suggestedName = "backup" + ext
	} else if strings.ToLower(filepath.Ext(suggestedName)) != ext {
		suggestedName = strings.TrimSuffix(suggestedName, filepath.Ext(suggestedName)) + ext
	}
	return wailsruntime.SaveFileDialog(a.ctx, wailsruntime.SaveDialogOptions{
		Title: "保存磁盘镜像", DefaultFilename: suggestedName,
		CanCreateDirectories: true,
		Filters:              []wailsruntime.FileFilter{{DisplayName: filter, Pattern: "*" + ext}},
	})
}

func (a *GUIApp) StartBackup(request StartBackupRequest) error {
	a.operationMu.Lock()
	defer a.operationMu.Unlock()

	output := strings.TrimSpace(request.Output)
	if output == "" {
		return fmt.Errorf("请选择镜像保存位置")
	}
	if request.BufferMB < 1 || request.BufferMB > 256 {
		return fmt.Errorf("缓冲区大小必须在 1 到 256 MB 之间")
	}
	if info, err := os.Stat(output); err == nil {
		if info.IsDir() {
			return fmt.Errorf("输出路径不能是目录")
		}
		if !request.Overwrite {
			return fmt.Errorf("输出文件已存在")
		}
		_ = os.Chmod(output, 0644)
	} else if !os.IsNotExist(err) {
		return err
	}
	if parent := filepath.Dir(output); parent != "." {
		if info, err := os.Stat(parent); err != nil || !info.IsDir() {
			return fmt.Errorf("输出目录不存在")
		}
	}

	a.mu.Lock()
	if a.backupRunning {
		a.mu.Unlock()
		return fmt.Errorf("已有备份任务正在运行")
	}
	if a.backend == nil || a.disk == nil {
		a.mu.Unlock()
		return fmt.Errorf("请先连接远程主机并扫描磁盘")
	}
	backend := a.backend
	disk := cloneDiskInfo(a.disk)
	ctx, cancel := context.WithCancel(a.ctx)
	a.backupRunning = true
	a.backupCancel = cancel
	a.mu.Unlock()

	modes := make(map[int]string, len(request.Partitions))
	for _, partition := range request.Partitions {
		modes[partition.Number] = strings.ToLower(partition.Mode)
	}
	for i := range disk.Partitions {
		mode := modes[disk.Partitions[i].Number]
		switch mode {
		case "skip":
			disk.Partitions[i].CopyMode = CopySkip
		case "used":
			if !disk.Partitions[i].FSType.SupportsBitmap() {
				cancel()
				a.finishBackupState()
				return fmt.Errorf("分区 #%d 不支持仅复制已用块", disk.Partitions[i].Number)
			}
			disk.Partitions[i].CopyMode = CopyUsedOnly
		default:
			disk.Partitions[i].CopyMode = CopyFull
		}
	}

	config := &ImagingConfig{
		Backend: backend, Disk: disk, Output: output,
		Format: FormatFromName(request.Format), BufSize: request.BufferMB * 1024 * 1024,
		Regions: BuildRegions(disk), Context: ctx,
		Observer: func(event ImagingEvent) {
			a.updateTaskbar(event)
			wailsruntime.EventsEmit(a.ctx, "backup:event", event)
		},
	}
	a.taskbar.indeterminate()
	go func() {
		err := RunImaging(config)
		done := BackupDoneEvent{Success: err == nil, Output: output}
		if err != nil {
			done.Cancelled = errors.Is(err, context.Canceled)
			if !done.Cancelled {
				done.Error = err.Error()
			}
		}
		if done.Success || done.Cancelled {
			a.taskbar.clear()
		} else {
			a.taskbar.failed(1, 1)
		}
		a.finishBackupState()
		wailsruntime.EventsEmit(a.ctx, "backup:done", done)
	}()
	return nil
}

func (a *GUIApp) updateTaskbar(event ImagingEvent) {
	if a.taskbar == nil {
		return
	}
	switch event.Kind {
	case "progress":
		a.taskbar.normal(event.Done, event.Total)
	case "reconnecting":
		a.taskbar.paused(event.Done, event.Total)
	case "stage":
		a.taskbar.indeterminate()
	case "log":
		if event.Stage == "reconnecting" && strings.Contains(event.Message, "继续备份") {
			a.taskbar.indeterminate()
		}
	}
}

func (a *GUIApp) finishBackupState() {
	a.mu.Lock()
	a.backupRunning = false
	a.backupCancel = nil
	a.mu.Unlock()
}

func cloneDiskInfo(source *DiskInfo) *DiskInfo {
	copyOfDisk := *source
	copyOfDisk.Partitions = append([]PartitionInfo(nil), source.Partitions...)
	return &copyOfDisk
}

func (a *GUIApp) CancelBackup() bool {
	a.mu.Lock()
	cancel := a.backupCancel
	conn := a.conn
	running := a.backupRunning
	a.mu.Unlock()
	if !running || cancel == nil {
		return false
	}
	cancel()
	// Closing SSH unblocks a pending network read; the imaging context prevents
	// the normal reconnect path from starting again.
	if conn != nil {
		conn.Close()
	}
	return true
}

func (a *GUIApp) RevealOutput(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("输出路径为空")
	}
	return exec.Command("explorer.exe", "/select,"+path).Start()
}

func runGUI() {
	app := NewGUIApp()
	err := wails.Run(&options.App{
		Title:            "SSH Imager",
		Width:            1040,
		Height:           680,
		MinWidth:         920,
		MinHeight:        620,
		AssetServer:      &assetserver.Options{Assets: guiAssets},
		BackgroundColour: &options.RGBA{R: 9, G: 16, B: 29, A: 1},
		OnStartup:        app.startup,
		OnShutdown:       app.shutdown,
		Bind:             []interface{}{app},
		Windows: &windows.Options{
			WebviewIsTransparent: false,
			WindowIsTranslucent:  false,
			Theme:                windows.SystemDefault,
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "GUI startup failed:", err)
	}
}
