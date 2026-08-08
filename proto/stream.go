package proto

import "io"

// Stream is one logical, ordered byte stream multiplexed over a
// Session's partitions. It implements io.ReadWriteCloser.
type Stream struct {
	ID        uint16
	SessionID uint16

	session *Session
	reader  *io.PipeReader
	writer  *io.PipeWriter
}

func newStream(session *Session, id uint16) *Stream {
	r, w := io.Pipe()
	return &Stream{
		ID:        id,
		SessionID: session.ID,
		session:   session,
		reader:    r,
		writer:    w,
	}
}

// Write sends p as a single FrameData frame on whichever partition
// this stream's ID resolves to (via Score).
func (s *Stream) Write(p []byte) (int, error) {
	if err := s.session.sendFrame(s.ID, FrameData, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Read blocks until data delivered by the session's RecvLoop is
// available; it carries the backpressure of the underlying pipe.
func (s *Stream) Read(p []byte) (int, error) {
	return s.reader.Read(p)
}

// Close signals the peer that no more data is coming and releases the
// stream locally.
func (s *Stream) Close() error {
	_ = s.session.sendFrame(s.ID, FrameFin, nil)
	s.writer.Close()
	return s.reader.Close()
}

// deliver is called by Session.RecvLoop to feed network data into the
// stream; it blocks until Read drains it.
func (s *Stream) deliver(data []byte) error {
	_, err := s.writer.Write(data)
	return err
}

// closeRemote is called by Session.RecvLoop when the peer sends a Fin
// (err == nil, subsequent Reads see io.EOF) or an RST (err != nil).
func (s *Stream) closeRemote(err error) {
	if err != nil {
		s.writer.CloseWithError(err)
		return
	}
	s.writer.Close()
}
