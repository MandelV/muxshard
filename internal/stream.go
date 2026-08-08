package internal

import (
	"muxshard/internal/protocol"
	"net"
	"time"
)

// Stream is one logical, ordered byte stream multiplexed over a
// Session's partitions. It implements net.Conn.
//
// A Stream has no single fixed underlying connection: which partition
// carries its next frame is recomputed on every Write (see
// Session.sendFrame), so LocalAddr/RemoteAddr resolve it dynamically
// instead of caching one picked at creation time.
type Stream struct {
	ID        uint16
	SessionID uint16

	session *Session
	local   net.Conn // app-facing end: Read/Write/Close/deadlines
	feed    net.Conn // fed by Session.RecvLoop via deliver()
}

func newStream(session *Session, id uint16) *Stream {
	local, feed := net.Pipe()
	return &Stream{
		ID:        id,
		SessionID: session.ID,
		session:   session,
		local:     local,
		feed:      feed,
	}
}

func (s *Stream) LocalAddr() net.Addr {
	if conn, err := s.session.partitionConnFor(s.ID); err == nil {
		return conn.LocalAddr()
	}
	return s.local.LocalAddr()
}

func (s *Stream) RemoteAddr() net.Addr {
	if conn, err := s.session.partitionConnFor(s.ID); err == nil {
		return conn.RemoteAddr()
	}
	return s.local.RemoteAddr()
}

// SetDeadline/SetReadDeadline/SetWriteDeadline bound Read, not Write:
// Write only enqueues a frame (see Write below), it never blocks on
// network I/O, so a write deadline has nothing to bound.
func (s *Stream) SetDeadline(t time.Time) error {
	return s.local.SetDeadline(t)
}
func (s *Stream) SetReadDeadline(t time.Time) error {
	return s.local.SetReadDeadline(t)
}
func (s *Stream) SetWriteDeadline(t time.Time) error {
	return s.local.SetWriteDeadline(t)
}

// Write sends p as a single FrameData frame on whichever partition
// this stream's ID resolves to (via Score).
//
// p is copied before it is handed off: sendFrame only enqueues the
// frame onto a channel and returns, well before SendLoop actually
// serializes it, so retaining p directly would race with callers
// (io.Copy in particular) that reuse their buffer as soon as Write
// returns — violating the io.Writer "must not retain p" contract and
// silently corrupting in-flight frames.
func (s *Stream) Write(p []byte) (int, error) {
	data := make([]byte, len(p))
	copy(data, p)

	if err := s.session.sendFrame(s.ID, protocol.FrameData, data); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Read blocks until data delivered by the session's RecvLoop is
// available; it carries the backpressure of the underlying pipe.
func (s *Stream) Read(p []byte) (int, error) {
	return s.local.Read(p)
}

// Close signals the peer that no more data is coming and releases the
// stream locally.
func (s *Stream) Close() error {
	_ = s.session.sendFrame(s.ID, protocol.FrameFin, nil)
	s.feed.Close()
	return s.local.Close()
}

// deliver is called by Session.RecvLoop to feed network data into the
// stream; it blocks until Read drains it.
func (s *Stream) deliver(data []byte) error {
	_, err := s.feed.Write(data)
	return err
}

// closeRemote is called by Session.RecvLoop when the peer sends a Fin
// or an RST; either way, further local Reads see io.EOF.
func (s *Stream) closeRemote() {
	s.feed.Close()
}
