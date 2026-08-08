package proto

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Type + Flags + SessionID + StreamID + Length + Reserved
const headerSize = 1 + 1 + 2 + 2 + 8 + 2

func WriteFrame(w io.Writer, f Frame) (n int, err error) {
	f.Header.Length = uint64(len(f.Data))

	buf := bytes.NewBuffer(make([]byte, 0, headerSize+len(f.Data)))
	if err := binary.Write(buf, binary.BigEndian, f.Header); err != nil {
		return -1, err
	}
	buf.Write(f.Data)

	return w.Write(buf.Bytes())
}

func ReadFrame(r io.Reader) (Frame, error) {
	var h Header
	if err := binary.Read(r, binary.BigEndian, &h); err != nil {
		return Frame{}, err
	}

	data := make([]byte, h.Length)
	if _, err := io.ReadFull(r, data); err != nil {
		return Frame{}, err
	}

	return Frame{Header: h, Data: data}, nil
}
