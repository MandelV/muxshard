package main

import (
	"fmt"
	"log"
	"muxshard"
	"muxshard/internal"
	"net"
)

func runServer(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	serv := muxshard.NewServer(ln)
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
		go greetClient(session)
	}
}

func acceptStreams(session *internal.Session) {
	for {
		stream := session.AcceptStream()
		go logStream(session.ID, stream)
	}
}

// greetClient opens a stream toward the client unprompted, to check
// the server->client direction works (client.acceptServerStreams is
// the matching AcceptStream on the other end).
func greetClient(session *internal.Session) {
	stream, err := session.OpenStream()
	if err != nil {
		log.Printf("server: session %d: open stream: %v", session.ID, err)
		return
	}

	msg := fmt.Sprintf("hello from server, session %d", session.ID)
	if _, err := stream.Write([]byte(msg)); err != nil {
		log.Printf("server: session %d: write stream %d: %v", session.ID, stream.ID, err)
	}
}

func logStream(sessionID uint16, stream *internal.Stream) {
	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			log.Printf("server: session %d: stream %d: %q", sessionID, stream.ID, buf[:n])

			stream.Write([]byte("coucou"))
		}
		if err != nil {
			return
		}
	}
}
