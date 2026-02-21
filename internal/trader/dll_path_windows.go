//go:build windows

package trader

import (
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

func init() {
	start, err := os.Getwd()
	if err != nil {
		return
	}

	dir := start
	for {
		wrap := filepath.Join(dir, "wrap.dll")
		trader := filepath.Join(dir, "thosttraderapi_se.dll")
		md := filepath.Join(dir, "thostmduserapi_se.dll")
		if fileExists(wrap) && fileExists(trader) && fileExists(md) {
			setDLLDirectory(dir)
			return
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return
		}
		dir = parent
	}
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func setDLLDirectory(path string) {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	proc := kernel32.NewProc("SetDllDirectoryW")
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return
	}
	_, _, _ = proc.Call(uintptr(unsafe.Pointer(p)))
}
