package main

import (
	"errors"
	"io"
	"log"
	"net"

	"muxshard/proto"
)

func runServer(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	log.Printf("server: listening on %s", addr)

	connIndex := 0
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		go handleConn(connIndex, conn)
		connIndex++
	}
}

func handleConn(index int, conn net.Conn) {
	defer conn.Close()

	log.Printf("server: conn #%d connected from %s", index, conn.RemoteAddr())

	for {
		h, payload, err := proto.ReadFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("server: conn #%d closed", index)
				return
			}
			log.Printf("server: conn #%d read error: %v", index, err)
			return
		}

		log.Printf("server: conn #%d StreamID=%d payload=%q", index, h.StreamID, payload)
	}
}
