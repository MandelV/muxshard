package main

import (
	"log"
	"net"

	"muxshard/proto"
	"muxshard/server"
)

func runServer(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	serv := server.NewServer(ln)
	defer serv.Close()

	log.Printf("server: listening on %s", addr)

	go func() {
		if err := serv.Serve(); err != nil {
			log.Printf("server: serve stopped: %v", err)
		}
	}()

	for {
		session := serv.AcceptSession()
		log.Printf("server: session %d accepted", session.ID)

		go acceptStreams(session)
	}
}

func acceptStreams(session *proto.Session) {
	for {
		stream := session.AcceptStream()
		go logStream(session.ID, stream)
	}
}

func logStream(sessionID uint16, stream *proto.Stream) {
	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			log.Printf("server: session %d: stream %d: %q", sessionID, stream.ID, buf[:n])
		}
		if err != nil {
			return
		}
	}
}
