//go:build windows

package main

import (
	"log"
	"os"
	"syscall"
	"unsafe"
)

func mmapFile(file *os.File) ([]byte, func(), error) {
	fi, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	fileSize := fi.Size()

	h, err := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, syscall.PAGE_READONLY, 0, 0, nil)
	if err != nil {
		log.Fatalf("Error creating file mapping: %v", err)
	}
	addr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, 0)
	if err != nil {
		log.Fatalf("Error mapping view of file: %v", err)
	}

	cleanup := func() {
		syscall.UnmapViewOfFile(addr)
		syscall.CloseHandle(h)
	}

	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), fileSize)

	return data, cleanup, nil
}
