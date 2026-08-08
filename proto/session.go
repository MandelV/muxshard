package proto

import (
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
)

/*
	Session

A Session multiplexes many logical Streams over several underlying
TCP connections, called partitions. Which partition carries a given
stream is decided once by Score(RoutinSeed, StreamID, PartitionCount)
and never changes for the life of that stream.
*/
type Session struct {
	ID             uint16
	RoutinSeed     uint16
	PartitionCount uint16

	partitions  []*partitionConn
	partitionMU sync.Mutex

	streamsChan chan *Stream
	streams     sync.Map

	streamIDMu   sync.Mutex
	nextStreamID uint16
}

type partitionConn struct {
	conn net.Conn
	send chan Frame
}

// NewSession creates a Session ready to accept and open streams.
// clientSide picks the parity of locally-opened stream IDs (odd for
// clients, even for servers), matching the convention on Header.
func NewSession(id, routinSeed, partitionCount uint16, clientSide bool) *Session {
	start := uint16(2)
	if clientSide {
		start = 1
	}

	return &Session{
		ID:             id,
		RoutinSeed:     routinSeed,
		PartitionCount: partitionCount,
		streamsChan:    make(chan *Stream, 32),
		nextStreamID:   start,
	}
}

// AddPartition registers conn as a new partition of the session and
// returns its index. Callers are expected to also run SendLoop and
// RecvLoop on conn.
func (s *Session) AddPartition(conn net.Conn) int {
	s.partitionMU.Lock()
	defer s.partitionMU.Unlock()

	s.partitions = append(s.partitions, &partitionConn{conn: conn, send: make(chan Frame, 16)})

	return len(s.partitions) - 1
}

// RemovePartition unregisters conn from the session and stops its
// SendLoop by closing its send channel.
//
// Note: this shifts the index of every partition after conn, so any
// stream whose Score() previously resolved to one of those higher
// indices will silently start targeting a different physical
// connection. Fine for a partition set that only shrinks at session
// teardown; not safe yet for partitions churning mid-session.
func (s *Session) RemovePartition(conn net.Conn) {
	s.partitionMU.Lock()
	defer s.partitionMU.Unlock()

	i := slices.IndexFunc(s.partitions, func(pc *partitionConn) bool { return pc.conn == conn })
	if i < 0 {
		return
	}

	close(s.partitions[i].send)
	s.partitions = slices.Delete(s.partitions, i, i+1)
}

// Close stops every partition's SendLoop by closing its send channel.
// Each SendLoop drains pending frames and closes its own net.Conn.
func (s *Session) Close() error {
	s.partitionMU.Lock()
	defer s.partitionMU.Unlock()

	for _, pc := range s.partitions {
		close(pc.send)
	}
	s.partitions = nil

	return nil
}

func (s *Session) send(partition uint32, f Frame) error {
	s.partitionMU.Lock()
	if int(partition) >= len(s.partitions) {
		s.partitionMU.Unlock()
		return fmt.Errorf("proto: session %d: unknown partition %d", s.ID, partition)
	}
	pc := s.partitions[partition]
	s.partitionMU.Unlock()

	pc.send <- f
	return nil
}

func (s *Session) sendFrame(streamID uint16, frameType FrameType, data []byte) error {
	partition := Score(uint64(s.RoutinSeed), uint64(streamID), uint64(s.PartitionCount))
	return s.send(uint32(partition), Frame{
		Header: Header{Type: frameType, SessionID: s.ID, StreamID: streamID},
		Data:   data,
	})
}

// getOrCreateStream returns the Stream for streamID, creating it (and
// surfacing it on AcceptStream) if this is the first frame seen for it.
func (s *Session) getOrCreateStream(streamID uint16) *Stream {
	candidate := newStream(s, streamID)

	actual, loaded := s.streams.LoadOrStore(streamID, candidate)
	stream := actual.(*Stream)

	if !loaded {
		s.streamsChan <- stream
	}

	return stream
}

// OpenStream allocates a new locally-initiated stream.
func (s *Session) OpenStream() (*Stream, error) {
	s.streamIDMu.Lock()
	id := s.nextStreamID
	s.nextStreamID += 2
	s.streamIDMu.Unlock()

	stream := newStream(s, id)
	if _, loaded := s.streams.LoadOrStore(id, stream); loaded {
		return nil, fmt.Errorf("proto: session %d: stream id %d already in use", s.ID, id)
	}

	return stream, nil
}

// AcceptStream blocks until a peer opens a new stream on this session.
func (s *Session) AcceptStream() *Stream {
	return <-s.streamsChan
}

func (s *Session) handleStreamMessage(f Frame) {
	switch f.Header.Type {
	case FrameData:
		_ = s.getOrCreateStream(f.Header.StreamID).deliver(f.Data)
	case FrameFin:
		if v, ok := s.streams.Load(f.Header.StreamID); ok {
			v.(*Stream).closeRemote(nil)
		}
	case FrameRST:
		if v, ok := s.streams.Load(f.Header.StreamID); ok {
			v.(*Stream).closeRemote(io.ErrClosedPipe)
		}
	}
}

// RecvLoop reads frames off conn (one partition's physical connection)
// until it closes, dispatching them to their stream or handling them
// as session-level control messages. It returns nil on a clean EOF.
func (s *Session) RecvLoop(conn net.Conn, partition uint32) error {
	for {
		f, err := ReadFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("partition %d: read error: %w", partition, err)
		}

		switch f.Header.Type {
		case FrameData, FrameFin, FrameRST:
			s.handleStreamMessage(f)
		case FrameGoAway:
			return nil
		case FramePing:
			if err := s.send(partition, Frame{Header: Header{Type: FramePong, SessionID: s.ID}}); err != nil {
				return err
			}
		}
	}
}

// SendLoop serializes every Frame enqueued for partition onto its
// connection; it exits and closes conn once the partition is removed
// (its send channel closed) and drained.
func (s *Session) SendLoop(conn net.Conn, partition uint32) error {
	s.partitionMU.Lock()
	if int(partition) >= len(s.partitions) {
		s.partitionMU.Unlock()
		return fmt.Errorf("proto: session %d: unknown partition %d", s.ID, partition)
	}
	pc := s.partitions[partition]
	s.partitionMU.Unlock()

	for frame := range pc.send {
		if _, err := WriteFrame(conn, frame); err != nil {
			return err
		}
	}

	return conn.Close()
}
