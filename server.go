package main

import (
	"log"
	"net"

	"muxshard/server"
)

func runServer(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	serv := &server.Server{Listener: ln}
	defer serv.Close()

	log.Printf("server: listening on %s", addr)
	return serv.Serve()
}
