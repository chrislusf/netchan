package util

import (
	"io"
)

func ReadBytes(r io.Reader, lenBuf []byte) (data []byte, err error) {
	_, err = io.ReadAtLeast(r, lenBuf, 4)
	if err == io.EOF {
		return
	}
	size := BytesToUint32(lenBuf)
	data = make([]byte, int(size))
	if size == 0 {
		return
	}
	c, err := io.ReadAtLeast(r, data, int(size))
	return data[:c], err
}

func WriteBytes(w io.Writer, lenBuf, data []byte) {
	size := len(data)
	Uint32toBytes(lenBuf, uint32(size))
	w.Write(lenBuf)
	w.Write(data)
}
