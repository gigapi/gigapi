package utils

import (
	"io"
	"sync"
)

var internalData sync.Map

func SetInternal(id string, r io.ReadSeeker) {
	internalData.Store(id, r)
}

func DelInternal(id string) {
	internalData.Delete(id)
}

func GetInternal(id string) io.ReadSeeker {
	res, _ := internalData.Load(id)
	if res == nil {
		return nil
	}
	return res.(io.ReadSeeker)
}
