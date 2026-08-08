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

A Session is holding underlying connexion (called partition)
*/
type Session struct {
	ID             uint16
	RoutinSeed     uint16
	PartitionCount uint16
	partitions     []net.Conn
	partitionMU    sync.Mutex

	streamsChan chan *Stream
	streams     sync.Map
}

// AddPartition registers conn as a new partition of the session and
// returns its index among the session's current partitions.
func (s *Session) AddPartition(conn net.Conn) int {
	s.partitionMU.Lock()
	defer s.partitionMU.Unlock()

	s.partitions = append(s.partitions, conn)
	s.PartitionCount = uint16(len(s.partitions))

	return len(s.partitions) - 1
}

func (s *Session) RemovePartition(conn net.Conn) {
	s.partitionMU.Lock()
	defer s.partitionMU.Unlock()

	if i := slices.Index(s.partitions, conn); i >= 0 {
		s.partitions = slices.Delete(s.partitions, i, i+1)
	}
	s.PartitionCount = uint16(len(s.partitions))
}

// getOrCreateStream handle When new stream arrive
func (s *Session) getOrCreateStream(streamID uint16) *Stream {

	actual, _ := s.streams.LoadOrStore(streamID, &Stream{ID: streamID})
	stream := actual.(*Stream)

	return stream
}

func (s *Session) RecvLoop(conn net.Conn, partition uint32) error {
	for {
		f, err := ReadFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {

				return fmt.Errorf("server: session %d: partition %d closed", s.ID, partition)
			}

			return fmt.Errorf("server: session %d: partition %d read error: %v", s.ID, partition, err)
		}

		switch f.Header.Type {
		case FrameData:

		case FrameFin:
			conn.Close()
		case FrameGoAway:
			conn.Close()
		case FramePing:

		}

	}
}

func (s *Session) SendLoop(conn net.Conn, partition uint32) error {
	return nil
}

func (s *Session) handleStreamMessage(hdr Header) {

}

func (s *Session) AcceptStream() *Stream {

	return <-s.streamsChan
}

func (s *Stream) OpenStream() (*Stream, error) {
	return nil, errors.ErrUnsupported
}
