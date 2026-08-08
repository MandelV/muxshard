package main

import (
	"fmt"
	"log"
	"muxshard"
	"muxshard/internal"
	"sync"
)

const demoStreamCount = 10

func runClient(addr string, partitionCount int) error {
	client, err := muxshard.NewClient(addr, partitionCount)
	if err != nil {
		return err
	}
	log.Printf("client: session RoutinSeed=%d, %d connections open", client.RoutinSeed, client.GetCurrentPartition())

	go acceptServerStreams(client.Session)

	var wg sync.WaitGroup
	for i := 0; i < demoStreamCount; i++ {
		stream, err := client.OpenStream()
		if err != nil {
			return fmt.Errorf("client: open stream %d: %w", i, err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			partition := internal.Shard(uint64(client.RoutinSeed), uint64(stream.ID), uint64(client.GetCurrentPartition()))
			log.Printf("client: stream %d -> partition %d", stream.ID, partition)

			if _, err := stream.Write([]byte(fmt.Sprintf("hello from stream %d", stream.ID))); err != nil {
				log.Printf("client: write stream %d: %v", stream.ID, err)
			}
		}()
	}
	wg.Wait() // every Write() has enqueued its frame

	return client.Close() // waits for every SendLoop to actually flush to the socket
}

// acceptServerStreams receives streams the server opens toward us
// (bidirectionality test): the client never asked for these, it just
// reacts as they arrive, same as the server does for ours.
func acceptServerStreams(session *internal.Session) {
	for {
		stream := session.AcceptStream()
		go logServerStream(stream)
	}
}

func logServerStream(stream *internal.Stream) {
	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			log.Printf("client: stream %d (from server): %q", stream.ID, buf[:n])
		}
		if err != nil {
			return
		}
	}
}
