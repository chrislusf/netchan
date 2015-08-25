package util

import (
	"io"
)

func ReadBytes(r io.Reader, lenBuf []byte) (flag ControlFlag, data []byte, err error) {
	_, err = io.ReadAtLeast(r, lenBuf[0:1], 1)
	if err == io.EOF {
		flag = FullStop
		return
	}
	// println("read  flag:", byte(flag))
	if ControlFlag(lenBuf[0]) != Data {
		flag = ControlFlag(lenBuf[0])
		return
	}
	_, err = io.ReadAtLeast(r, lenBuf, 4)
	if err == io.EOF {
		flag = FullStop
		return
	}
	size := BytesToUint32(lenBuf)
	// println("read size:", size)
	data = make([]byte, int(size))
	if size == 0 {
		return
	}
	c, err := io.ReadAtLeast(r, data, int(size))
	return Data, data[:c], err
}

func WriteBytes(w io.Writer, flag ControlFlag, lenBuf, data []byte) {
	lenBuf[0] = byte(flag)
	w.Write(lenBuf[0:1])
	// println("write flag:", byte(flag))
	if flag != Data {
		return
	}
	size := len(data)
	// println("write size:", size)
	Uint32toBytes(lenBuf, uint32(size))
	w.Write(lenBuf)
	w.Write(data)
}
