// Package muxshard is a yamux-style stream multiplexer where a single
// logical Session is carried over several TCP connections ("partitions")
// instead of just one. Server accepts partitions dialed in by Client and
// groups them by SessionID; Client dials and wires up a fixed number of
// partitions for one Session. Once established, a Session's OpenStream/
// AcceptStream work exactly like yamux's.
package muxshard

import (
	"log"
	"net"
	"sync"

	"muxshard/proto"
)

// Server accepts partitions dialed by Client and groups them into Sessions.
type Server struct {
	net.Listener
	Sessions    sync.Map
	sessionChan chan *proto.Session
	Drain       bool
}

func NewServer(l net.Listener) *Server {
	return &Server{
		Listener:    l,
		sessionChan: make(chan *proto.Session),
	}
}

func (s *Server) Serve() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	open, err := proto.ReadFrame(conn)
	if err != nil {
		log.Printf("muxshard: handshake read error from %s: %v", conn.RemoteAddr(), err)
		conn.Close()
		return
	}
	if open.Header.Type != proto.FrameSessionOpen {
		log.Printf("muxshard: expected FrameOpen from %s, got type %d", conn.RemoteAddr(), open.Header.Type)
		conn.Close()
		return
	}

	sessionID := open.Header.SessionID
	actual, loaded := s.Sessions.LoadOrStore(sessionID, proto.NewSession(sessionID, 0, false))
	session := actual.(*proto.Session)
	if !loaded {
		s.sessionChan <- session
	}

	partition := session.AddPartition(conn)
	log.Printf("muxshard: session %d: partition %d connected from %s", sessionID, partition, conn.RemoteAddr())

	// SendLoop owns conn's lifetime: it closes conn once RemovePartition
	// closes its send channel below, after RecvLoop has returned.
	go session.SendLoop(conn, uint32(partition))

	if err := session.RecvLoop(conn, uint32(partition)); err != nil {
		log.Printf("muxshard: session %d: partition %d: %v", sessionID, partition, err)
	}

	session.RemovePartition(conn)
}

// AcceptSession blocks until a new client session is established.
func (s *Server) AcceptSession() *proto.Session {
	return <-s.sessionChan
}
