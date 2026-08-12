//go:build windows && !cli

package main

import (
	"runtime"
	"syscall"
	"unsafe"

	"github.com/go-ole/go-ole"
	"golang.org/x/sys/windows"
)

// Windows taskbar progress states from TBPFLAG.
const (
	taskbarNoProgress    = 0x0
	taskbarIndeterminate = 0x1
	taskbarNormal        = 0x2
	taskbarError         = 0x4
	taskbarPaused        = 0x8
)

var (
	user32Proc       = windows.NewLazySystemDLL("user32.dll")
	findWindowWProc  = user32Proc.NewProc("FindWindowW")
	clsidTaskbarList = ole.NewGUID("{56FDF344-FD6D-11D0-958A-006097C9A090}")
	iidTaskbarList3  = ole.NewGUID("{EA1AFB91-9E28-4B86-90E9-9E9F8A5EEFAF}")
)

type taskbarList3 struct {
	lpVtbl *taskbarList3Vtbl
}

type taskbarList3Vtbl struct {
	QueryInterface        uintptr
	AddRef                uintptr
	Release               uintptr
	HrInit                uintptr
	AddTab                uintptr
	DeleteTab             uintptr
	ActivateTab           uintptr
	SetActiveAlt          uintptr
	MarkFullscreenWindow  uintptr
	SetProgressValue      uintptr
	SetProgressState      uintptr
	RegisterTab           uintptr
	UnregisterTab         uintptr
	SetTabOrder           uintptr
	SetTabActive          uintptr
	ThumbBarAddButtons    uintptr
	ThumbBarUpdateButtons uintptr
	ThumbBarSetImageList  uintptr
	SetOverlayIcon        uintptr
	SetThumbnailTooltip   uintptr
	SetThumbnailClip      uintptr
}

type taskbarCommand struct {
	state     uintptr
	completed uint64
	total     uint64
	hasValue  bool
}

// taskbarProgress serializes ITaskbarList3 calls onto one locked OS thread.
// COM interface pointers must not be used concurrently from imaging workers.
type taskbarProgress struct {
	commands chan taskbarCommand
	stop     chan struct{}
	done     chan struct{}
}

func newTaskbarProgress() *taskbarProgress {
	t := &taskbarProgress{
		commands: make(chan taskbarCommand, 8),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go t.run()
	return t
}

func (t *taskbarProgress) run() {
	defer close(t.done)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := ole.CoInitializeEx(0, ole.COINIT_APARTMENTTHREADED); err != nil {
		return
	}
	defer ole.CoUninitialize()

	unknown, err := ole.CreateInstance(clsidTaskbarList, iidTaskbarList3)
	if err != nil {
		return
	}
	taskbar := (*taskbarList3)(unsafe.Pointer(unknown))
	defer syscall.SyscallN(taskbar.lpVtbl.Release, uintptr(unsafe.Pointer(taskbar)))

	if result, _, _ := syscall.SyscallN(taskbar.lpVtbl.HrInit, uintptr(unsafe.Pointer(taskbar))); result != 0 {
		return
	}

	for {
		select {
		case command := <-t.commands:
			hwnd := findSSHImagerWindow()
			if hwnd == 0 {
				continue
			}
			if command.hasValue && command.total > 0 {
				completed := command.completed
				if completed > command.total {
					completed = command.total
				}
				syscall.SyscallN(taskbar.lpVtbl.SetProgressValue,
					uintptr(unsafe.Pointer(taskbar)), hwnd,
					uintptr(completed), uintptr(command.total))
			}
			syscall.SyscallN(taskbar.lpVtbl.SetProgressState,
				uintptr(unsafe.Pointer(taskbar)), hwnd, command.state)
		case <-t.stop:
			hwnd := findSSHImagerWindow()
			if hwnd != 0 {
				syscall.SyscallN(taskbar.lpVtbl.SetProgressState,
					uintptr(unsafe.Pointer(taskbar)), hwnd, taskbarNoProgress)
			}
			return
		}
	}
}

func findSSHImagerWindow() uintptr {
	title, err := windows.UTF16PtrFromString("SSH Imager")
	if err != nil {
		return 0
	}
	hwnd, _, _ := findWindowWProc.Call(0, uintptr(unsafe.Pointer(title)))
	return hwnd
}

func (t *taskbarProgress) send(command taskbarCommand) {
	if t == nil {
		return
	}
	// Progress is emitted several times per second. Retain the newest update
	// instead of allowing taskbar painting to back-pressure disk imaging.
	select {
	case t.commands <- command:
	default:
		select {
		case <-t.commands:
		default:
		}
		select {
		case t.commands <- command:
		default:
		}
	}
}

func (t *taskbarProgress) indeterminate() {
	t.send(taskbarCommand{state: taskbarIndeterminate})
}

func (t *taskbarProgress) normal(completed, total uint64) {
	t.send(taskbarCommand{state: taskbarNormal, completed: completed, total: total, hasValue: total > 0})
}

func (t *taskbarProgress) paused(completed, total uint64) {
	t.send(taskbarCommand{state: taskbarPaused, completed: completed, total: total, hasValue: total > 0})
}

func (t *taskbarProgress) failed(completed, total uint64) {
	t.send(taskbarCommand{state: taskbarError, completed: completed, total: total, hasValue: total > 0})
}

func (t *taskbarProgress) clear() {
	t.send(taskbarCommand{state: taskbarNoProgress})
}

func (t *taskbarProgress) close() {
	if t == nil {
		return
	}
	select {
	case <-t.done:
		return
	default:
	}
	close(t.stop)
	<-t.done
}
