package bkdtree

import (
	"encoding/json"
	"os"
	"syscall"

	"github.com/pkg/errors"
)

//https://medium.com/@arpith/adventures-with-mmap-463b33405223
func FileMmap(f *os.File) (data []byte, err error) {
	info, err1 := f.Stat()
	if err1 != nil {
		err = errors.Wrap(err1, "")
		return
	}
	prots := []int{syscall.PROT_WRITE | syscall.PROT_READ, syscall.PROT_READ}
	for _, prot := range prots {
		data, err = syscall.Mmap(int(f.Fd()), 0, int(info.Size()), prot, syscall.MAP_SHARED)
		if err == nil {
			break
		}
	}
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func FileMunmap(data []byte) (err error) {
	err = syscall.Munmap(data)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func FileUnmarshal(fp string, v interface{}) (err error) {
	var f *os.File
	var data []byte
	if f, err = os.Open(fp); err != nil {
		return
	}
	defer f.Close()
	if data, err = FileMmap(f); err != nil {
		return
	}
	defer FileMunmap(data)
	err = json.Unmarshal(data, v)
	return
}

func FileMarshal(fp string, v interface{}) (err error) {
	var f *os.File
	var data []byte
	var count int
	if data, err = json.Marshal(v); err != nil {
		return
	}
	if f, err = os.OpenFile(fp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600); err != nil {
		return
	}
	defer f.Close()
	if count, err = f.Write(data); err != nil {
		return
	}
	if count != len(data) {
		err = errors.Errorf("%s partial wirte %d, want %d", fp, count, len(data))
		return
	}
	return
}
