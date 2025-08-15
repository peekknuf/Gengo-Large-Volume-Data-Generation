//go:build !windows

package main

import (
	"os"
	"syscall"
)

func mmapFile(file *os.File) ([]byte, func(), error) {
	fi, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	fileSize := fi.Size()

	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		syscall.Munmap(data)
	}

	return data, cleanup, nil
}
