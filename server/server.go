package server

import (
	"log"
	"net"
	"sync"

	"muxshard/proto"
)

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
		log.Printf("server: handshake read error from %s: %v", conn.RemoteAddr(), err)
		conn.Close()
		return
	}
	if open.Header.Type != proto.FrameSessionOpen {
		log.Printf("server: expected FrameOpen from %s, got type %d", conn.RemoteAddr(), open.Header.Type)
		conn.Close()
		return
	}

	sessionID := open.Header.SessionID
	actual, loaded := s.Sessions.LoadOrStore(sessionID, proto.NewSession(sessionID, 0, 0, false))
	session := actual.(*proto.Session)
	if !loaded {
		s.sessionChan <- session
	}

	partition := session.AddPartition(conn)
	log.Printf("server: session %d: partition %d connected from %s", sessionID, partition, conn.RemoteAddr())

	// SendLoop owns conn's lifetime: it closes conn once RemovePartition
	// closes its send channel below, after RecvLoop has returned.
	go session.SendLoop(conn, uint32(partition))

	if err := session.RecvLoop(conn, uint32(partition)); err != nil {
		log.Printf("server: session %d: partition %d: %v", sessionID, partition, err)
	}

	session.RemovePartition(conn)
}

// AcceptSession blocks until a new client session is established.
func (s *Server) AcceptSession() *proto.Session {
	return <-s.sessionChan
}
