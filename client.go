package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net"

	"muxshard/proto"
)

const demoStreamCount = 10

func runClient(addr string, partitionCount int) error {
	conns := make([]net.Conn, partitionCount)
	for i := range conns {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("client: dial partition %d: %w", i, err)
		}
		defer conn.Close()

		conns[i] = conn
	}

	session := proto.Session{
		ID:         1,
		RoutinSeed: uint16(rand.IntN(1 << 16)),
	}
	log.Printf("client: session RoutinSeed=%d, %d connections open", session.RoutinSeed, partitionCount)

	for streamID := uint16(0); streamID < demoStreamCount; streamID++ {
		partition := proto.Score(uint64(session.RoutinSeed), uint64(streamID), uint64(partitionCount))
		log.Printf("client: stream %d -> partition %d", streamID, partition)

		payload := []byte(fmt.Sprintf("hello from stream %d", streamID))
		header := proto.Header{Type: 1, StreamID: streamID}

		if err := proto.WriteFrame(conns[partition], header, payload); err != nil {
			return fmt.Errorf("client: write stream %d on partition %d: %w", streamID, partition, err)
		}
	}

	return nil
}
