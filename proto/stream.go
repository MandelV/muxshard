package proto

import "io"

type Stream struct {
	io.Reader
	io.Writer
	io.Closer
	ID        uint16
	SessionID uint16
}

func NewStream()

func (s *Stream) Write(p []byte) (n int, err error) {

	return WriteFrame(s.Writer, Frame{Header: Header{Type: FrameData, StreamID: s.ID, SessionID: s.SessionID}, Data: p})
}

func (s *Stream) Read(p []byte) (n int, err error) {

	ReadFrame(s.Reader)

	return 0, nil
}
