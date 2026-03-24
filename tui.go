package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// TUIDiskSelect shows a disk selection UI. Returns selected device path or empty.
func TUIDiskSelect(disks []RemoteDisk, userHost string) string {
	if len(disks) == 0 {
		fmt.Println("No disks found on remote.")
		return ""
	}

	app := tview.NewApplication()
	selected := ""

	// Header
	header := tview.NewTextView().
		SetDynamicColors(true).
		SetText(fmt.Sprintf(" [yellow]sshimager v%s[-]\n Remote: [green]%s[-]\n Select a disk to image:",
			Version, userHost))

	// Disk table
	table := tview.NewTable().
		SetSelectable(true, false).
		SetFixed(1, 0)

	// Header row
	for i, h := range []string{"Device", "Size", "Model"} {
		table.SetCell(0, i, tview.NewTableCell(h).
			SetTextColor(tcell.ColorYellow).
			SetSelectable(false))
	}

	for i, d := range disks {
		row := i + 1
		table.SetCell(row, 0, tview.NewTableCell(d.Dev))
		table.SetCell(row, 1, tview.NewTableCell(FormatSize(d.Size)).SetAlign(tview.AlignRight))
		table.SetCell(row, 2, tview.NewTableCell(d.Model))
	}
	table.SetBorder(true).SetTitle(" Remote Disks ")

	// Select on Enter
	table.SetSelectedFunc(func(row, column int) {
		idx := row - 1
		if idx >= 0 && idx < len(disks) {
			selected = disks[idx].Dev
			app.Stop()
		}
	})

	// Esc to quit
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			app.Stop()
			return nil
		}
		return event
	})

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(header, 4, 0, false).
		AddItem(table, 0, 1, true)

	footer := tview.NewTextView().
		SetDynamicColors(true).
		SetText(" [gray] Arrow keys: select  Enter: confirm  Esc: quit[-]")
	layout.AddItem(footer, 1, 0, false)

	if err := app.SetRoot(layout, true).EnableMouse(true).Run(); err != nil {
		return ""
	}

	return selected
}

// TUIPartitionConfig shows partition configuration UI.
// Returns output path, format, or false if cancelled.
func TUIPartitionConfig(disk *DiskInfo, outputPreset string) (string, VDiskFormat, bool) {
	app := tview.NewApplication()

	outputPath := outputPreset
	if outputPath == "" {
		outputPath = "/tmp/disk.vmdk"
	}
	selectedFormat := FormatVMDK
	confirmed := false

	// ── Disk info header ──
	ptType := "MBR"
	if disk.PTType == PTGPT {
		ptType = "GPT"
	}
	header := tview.NewTextView().
		SetDynamicColors(true).
		SetText(fmt.Sprintf(
			" [yellow]sshimager v%s[-]\n"+
				" Disk: [green]%s[-]  %s  %s  %s  %d partitions\n"+
				" [gray]Space: toggle select | Enter: toggle mode | Tab: switch focus | Esc: quit[-]",
			Version, disk.DevPath, disk.Model, FormatSize(disk.Size),
			ptType, len(disk.Partitions)))

	// ── Partition table ──
	table := tview.NewTable().
		SetSelectable(true, false).
		SetFixed(1, 0)

	for i, h := range []string{"Sel", "Device", "Type", "Size", "Mount", "Copy Mode"} {
		cell := tview.NewTableCell(h).
			SetTextColor(tcell.ColorYellow).
			SetSelectable(false)
		if i == 3 {
			cell.SetAlign(tview.AlignRight)
		}
		table.SetCell(0, i, cell)
	}

	refreshTable := func() {
		for i, p := range disk.Partitions {
			row := i + 1

			sel := "[green::b] Y [-::-]"
			if p.CopyMode == CopySkip {
				sel = "[red::b] N [-::-]"
			}

			mode := "[white]Full"
			if p.CopyMode == CopySkip {
				mode = "[gray]--skip--"
			} else if p.CopyMode == CopyUsedOnly {
				mode = "[cyan]Used-only"
			}

			mount := p.Mountpoint
			if mount == "" {
				mount = p.FSLabel
			}

			table.SetCell(row, 0, tview.NewTableCell(sel).SetAlign(tview.AlignCenter))
			table.SetCell(row, 1, tview.NewTableCell(p.DevPath))
			table.SetCell(row, 2, tview.NewTableCell(p.FSType.String()))
			table.SetCell(row, 3, tview.NewTableCell(FormatSize(p.Size)).SetAlign(tview.AlignRight))
			table.SetCell(row, 4, tview.NewTableCell(mount))
			table.SetCell(row, 5, tview.NewTableCell(mode))
		}
	}
	refreshTable()
	table.SetBorder(true).SetTitle(" Partitions ")

	// ── Output input field ──
	outputField := tview.NewInputField().
		SetLabel(" Output: ").
		SetText(outputPath).
		SetFieldWidth(50).
		SetChangedFunc(func(text string) {
			outputPath = text
		})

	// ── Format dropdown ──
	formatOptions := []string{"VMDK", "VHD", "VDI", "DD"}
	formatDrop := tview.NewDropDown().
		SetLabel(" Format: ").
		SetOptions(formatOptions, func(text string, index int) {
			switch index {
			case 0:
				selectedFormat = FormatVMDK
			case 1:
				selectedFormat = FormatVHD
			case 2:
				selectedFormat = FormatVDI
			case 3:
				selectedFormat = FormatDD
			}
			ext := ".vmdk"
			switch selectedFormat {
			case FormatVHD:
				ext = ".vhd"
			case FormatVDI:
				ext = ".vdi"
			case FormatDD:
				ext = ".dd"
			}
			if e := filepath.Ext(outputPath); e != "" {
				outputPath = strings.TrimSuffix(outputPath, e) + ext
			}
			outputField.SetText(outputPath)
		}).
		SetCurrentOption(0)

	// ── Buttons ──
	startBtn := tview.NewButton(" START ").SetSelectedFunc(func() {
		confirmed = true
		app.Stop()
	})
	startBtn.SetBackgroundColor(tcell.ColorGreen)
	startBtn.SetLabelColor(tcell.ColorBlack)

	quitBtn := tview.NewButton(" Quit ").SetSelectedFunc(func() {
		app.Stop()
	})
	quitBtn.SetBackgroundColor(tcell.ColorDarkRed)
	quitBtn.SetLabelColor(tcell.ColorWhite)

	btnRow := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(startBtn, 10, 0, false).
		AddItem(tview.NewBox(), 2, 0, false).
		AddItem(quitBtn, 8, 0, false).
		AddItem(tview.NewBox(), 0, 1, false)

	// ── Settings panel ──
	settingsPanel := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(outputField, 1, 0, false).
		AddItem(formatDrop, 1, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(btnRow, 1, 0, false)
	settingsPanel.SetBorder(true).SetTitle(" Output Settings ")

	// ── Layout ──
	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(header, 4, 0, false).
		AddItem(table, 0, 1, true).
		AddItem(settingsPanel, 7, 0, false)

	// ── Focus management ──
	focusItems := []tview.Primitive{table, outputField, formatDrop, startBtn, quitBtn}
	focusIdx := 0

	// Single global key handler
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyTab:
			focusIdx = (focusIdx + 1) % len(focusItems)
			app.SetFocus(focusItems[focusIdx])
			return nil
		case tcell.KeyBacktab:
			focusIdx = (focusIdx - 1 + len(focusItems)) % len(focusItems)
			app.SetFocus(focusItems[focusIdx])
			return nil
		case tcell.KeyEsc:
			app.Stop()
			return nil
		}

		// Partition operations when table is focused
		if app.GetFocus() == table {
			row, _ := table.GetSelection()
			idx := row - 1
			if idx >= 0 && idx < len(disk.Partitions) {
				p := &disk.Partitions[idx]

				if event.Key() == tcell.KeyRune && event.Rune() == ' ' {
					if p.CopyMode == CopySkip {
						p.CopyMode = CopyFull
					} else {
						p.CopyMode = CopySkip
					}
					refreshTable()
					return nil
				}
				if event.Key() == tcell.KeyEnter {
					if p.CopyMode != CopySkip && p.FSType.SupportsBitmap() {
						if p.CopyMode == CopyFull {
							p.CopyMode = CopyUsedOnly
						} else {
							p.CopyMode = CopyFull
						}
						refreshTable()
					}
					return nil
				}
			}
		}

		return event
	})

	if err := app.SetRoot(layout, true).EnableMouse(true).Run(); err != nil {
		return "", FormatVMDK, false
	}

	if !confirmed {
		return "", FormatVMDK, false
	}

	return outputPath, selectedFormat, true
}
