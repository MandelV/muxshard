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
		Drain:       false,
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
	actual, _ := s.Sessions.LoadOrStore(sessionID, &proto.Session{ID: sessionID})
	session := actual.(*proto.Session)

	partition := session.AddPartition(conn)
	log.Printf("server: session %d: partition %d connected from %s (%d partitions)", sessionID, partition, conn.RemoteAddr(), session.PartitionCount)

	defer func() {
		session.RemovePartition(conn)
		conn.Close()
	}()

	go session.RecvLoop(conn, uint32(partition))
	go session.SendLoop(conn, uint32(partition))

}

func (s *Server) AcceptSession() *proto.Session {
	return <-s.sessionChan
}
