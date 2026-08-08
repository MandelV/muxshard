package proto

import (
	"encoding/binary"
	"io"
)

func WriteFrame(w io.Writer, h Header, payload []byte) error {
	h.Length = uint64(len(payload))

	if err := binary.Write(w, binary.BigEndian, h); err != nil {
		return err
	}

	_, err := w.Write(payload)
	return err
}

func ReadFrame(r io.Reader) (Header, []byte, error) {
	var h Header
	if err := binary.Read(r, binary.BigEndian, &h); err != nil {
		return Header{}, nil, err
	}

	payload := make([]byte, h.Length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Header{}, nil, err
	}

	return h, payload, nil
}
