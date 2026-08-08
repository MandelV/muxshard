package main

import (
	"fmt"
	"log"
	"net"
	"sync"

	"muxshard/proto"
)

const demoStreamCount = 10

func runClient(addr string, partitionCount int) error {
	session := proto.Session{
		ID:             1,
		RoutinSeed:     65412, //uint16(rand.IntN(1 << 16))
		PartitionCount: uint16(partitionCount),
	}

	conns := make([]net.Conn, partitionCount)
	for i := range conns {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("client: dial partition %d: %w", i, err)
		}
		defer conn.Close()

		open := proto.Frame{Header: proto.Header{Type: proto.FrameSessionOpen, SessionID: session.ID}}
		if _, err := proto.WriteFrame(conn, open); err != nil {
			return fmt.Errorf("client: open handshake on partition %d: %w", i, err)
		}

		conns[i] = conn
	}
	log.Printf("client: session RoutinSeed=%d, %d connections open", session.RoutinSeed, session.PartitionCount)

	var wg sync.WaitGroup
	for streamID := uint16(0); streamID < demoStreamCount; streamID++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			partition := proto.Score(uint64(session.RoutinSeed), uint64(streamID), uint64(session.PartitionCount))
			log.Printf("client: stream %d -> partition %d", streamID, partition)

			frame := proto.Frame{
				Header: proto.Header{Type: proto.FrameData, SessionID: session.ID, StreamID: streamID},
				Data:   []byte(fmt.Sprintf("hello from stream %d", streamID)),
			}

			if _, err := proto.WriteFrame(conns[partition], frame); err != nil {
				log.Printf("client: write stream %d on partition %d: %v", streamID, partition, err)
			}
		}()
	}
	wg.Wait()

	return nil
}
