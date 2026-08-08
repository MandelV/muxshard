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
TCP connections, called partitions. Session itself is agnostic about
how many partitions there should be — it only tracks how many are
currently live (CurrentPartition) and routes with whatever that is.
Keeping the live count matched to a target is the caller's job: the
server is a listener, its partitions simply arrive as clients dial in;
the client is the one that knows how many it wants open and will be
responsible for reconciling CurrentPartition against that target
(not implemented yet).
*/
type Session struct {
	ID               uint16
	RoutinSeed       uint16
	CurrentPartition int

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

	// mu/closed guard send against racing with RemovePartition/Close:
	// without them, a send could land on pc.send after it's been
	// closed, which panics.
	mu     sync.Mutex
	closed bool
}

// NewSession creates a Session ready to accept and open streams.
// clientSide picks the parity of locally-opened stream IDs (odd for
// clients, even for servers), matching the convention on Header.
func NewSession(id, routinSeed uint16, clientSide bool) *Session {
	start := uint16(2)
	if clientSide {
		start = 1
	}

	return &Session{
		ID:           id,
		RoutinSeed:   routinSeed,
		streamsChan:  make(chan *Stream, 1024),
		nextStreamID: start,
	}
}

// AddPartition registers conn as a new partition of the session and
// returns its index. Callers are expected to also run SendLoop and
// RecvLoop on conn.
func (s *Session) AddPartition(conn net.Conn) int {
	s.partitionMU.Lock()
	defer s.partitionMU.Unlock()

	s.partitions = append(s.partitions, &partitionConn{conn: conn, send: make(chan Frame, 64)})
	s.CurrentPartition = len(s.partitions)

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
func (s *Session) RemovePartition(conn net.Conn) int {
	s.partitionMU.Lock()
	i := slices.IndexFunc(s.partitions, func(pc *partitionConn) bool { return pc.conn == conn })
	if i < 0 {
		count := len(s.partitions)
		s.partitionMU.Unlock()
		return count
	}
	pc := s.partitions[i]
	s.partitions = slices.Delete(s.partitions, i, i+1)
	s.CurrentPartition = len(s.partitions)
	count := s.CurrentPartition
	s.partitionMU.Unlock()

	pc.mu.Lock()
	pc.closed = true
	close(pc.send)
	pc.mu.Unlock()

	return count
}

// Close stops every partition's SendLoop by closing its send channel.
// Each SendLoop drains pending frames and closes its own net.Conn.
func (s *Session) Close() error {
	s.partitionMU.Lock()
	partitions := s.partitions
	s.partitions = nil
	s.CurrentPartition = 0
	s.partitionMU.Unlock()

	for _, pc := range partitions {
		pc.mu.Lock()
		pc.closed = true
		close(pc.send)
		pc.mu.Unlock()
	}

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

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed {
		return fmt.Errorf("proto: session %d: partition %d closed", s.ID, partition)
	}

	pc.send <- f
	return nil
}

func (s *Session) sendFrame(streamID uint16, frameType FrameType, data []byte) error {
	partition := Score(uint64(s.RoutinSeed), uint64(streamID), uint64(s.CurrentPartition))
	return s.send(uint32(partition), Frame{
		Header: Header{Type: frameType, SessionID: s.ID, StreamID: streamID},
		Data:   data,
	})
}

// partitionConnFor returns the net.Conn a stream's frames are
// currently routed to — same Score() computation as sendFrame, so it
// always reflects where the next Write would actually go.
func (s *Session) partitionConnFor(streamID uint16) (net.Conn, error) {
	s.partitionMU.Lock()
	defer s.partitionMU.Unlock()

	partition := Score(uint64(s.RoutinSeed), uint64(streamID), uint64(s.CurrentPartition))
	if int(partition) >= len(s.partitions) {
		return nil, fmt.Errorf("proto: session %d: unknown partition %d", s.ID, partition)
	}
	return s.partitions[partition].conn, nil
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
	case FrameFin, FrameRST:
		if v, ok := s.streams.Load(f.Header.StreamID); ok {
			v.(*Stream).closeRemote()
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
