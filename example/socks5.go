package main

import (
	"io"
	"log"
	"net"

	"muxshard"

	"github.com/things-go/go-socks5"
)

func runSock5() {
	ln, err := net.Listen("tcp", "127.0.0.1:9000")
	if err != nil {
		return
	}
	serv := muxshard.NewServer(ln)

	ss5 := socks5.NewServer()

	go func() {
		if err := serv.Serve(); err != nil {
			log.Printf("server: serve stopped: %v", err)
		}
	}()

	for {
		session := serv.AcceptSession()

		log.Println("new Session")

		go func() {
			for {
				stream := session.AcceptStream()

				log.Printf("new stream %d", stream.ID)

				go ss5.ServeConn(stream)
			}

		}()

	}
}

func startClient() {
	ln, err := net.Listen("tcp", "127.0.0.1:9001")
	if err != nil {
		return
	}

	client, err := muxshard.NewClient("127.0.0.1:9000", 16)
	if err != nil {
		panic(err)
	}

	for {

		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		stream, err := client.OpenStream()
		if err != nil {
			log.Println(stream)
			continue
		}

		go func() {
			io.Copy(conn, stream) // download: peer -> browser
			conn.Close()
		}()
		go func() {
			io.Copy(stream, conn) // upload: browser -> peer
			stream.Close()
		}()

	}

}

func main() {
	go runSock5()

	startClient()
}
