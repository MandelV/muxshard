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
	session := proto.NewSession(1, 65412, uint16(partitionCount), true) //uint16(rand.IntN(1 << 16))

	var sendWG sync.WaitGroup
	for i := 0; i < partitionCount; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("client: dial partition %d: %w", i, err)
		}

		open := proto.Frame{Header: proto.Header{Type: proto.FrameSessionOpen, SessionID: session.ID}}
		if _, err := proto.WriteFrame(conn, open); err != nil {
			return fmt.Errorf("client: open handshake on partition %d: %w", i, err)
		}

		partition := session.AddPartition(conn)

		sendWG.Add(1)
		go func() {
			defer sendWG.Done()
			if err := session.SendLoop(conn, uint32(partition)); err != nil {
				log.Printf("client: partition %d send: %v", partition, err)
			}
		}()
		go func() {
			if err := session.RecvLoop(conn, uint32(partition)); err != nil {
				log.Printf("client: partition %d recv: %v", partition, err)
			}
			session.RemovePartition(conn)
		}()
	}
	log.Printf("client: session RoutinSeed=%d, %d connections open", session.RoutinSeed, session.PartitionCount)

	var wg sync.WaitGroup
	for i := 0; i < demoStreamCount; i++ {
		stream, err := session.OpenStream()
		if err != nil {
			return fmt.Errorf("client: open stream %d: %w", i, err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			partition := proto.Score(uint64(session.RoutinSeed), uint64(stream.ID), uint64(session.PartitionCount))
			log.Printf("client: stream %d -> partition %d", stream.ID, partition)

			if _, err := stream.Write([]byte(fmt.Sprintf("hello from stream %d", stream.ID))); err != nil {
				log.Printf("client: write stream %d: %v", stream.ID, err)
			}
		}()
	}
	wg.Wait() // every Write() has enqueued its frame

	session.Close() // close each partition's send channel
	sendWG.Wait()   // wait until every SendLoop has actually flushed to the socket

	return nil
}
