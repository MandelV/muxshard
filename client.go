package muxshard

import (
	"fmt"
	"log"
	"math/rand/v2"
	"muxshard/internal"
	"muxshard/internal/protocol"
	"net"
	"sync"
	"sync/atomic"
)

// PartitionDown reports that one of a Client's partitions stopped
// carrying traffic unexpectedly (its RecvLoop returned).
type PartitionDown struct {
	Partition int
	Err       error
}

// Client dials and manages the partitions of a Session it opened.
// *Client promotes *proto.Session, so OpenStream/AcceptStream work
// directly on it.
type Client struct {
	*internal.Session

	sendWG  sync.WaitGroup
	closing atomic.Bool

	// PartitionDown receives an event whenever a partition drops
	// outside of Close (e.g. the underlying TCP connection died).
	// Reconciling CurrentPartition back up to a target by redialing is
	// not implemented yet — this only gives visibility into it happening.
	PartitionDown chan PartitionDown
}

// NewClient dials partitionCount TCP connections to addr, opens a new
// session (handshaking each connection as one of its partitions), and
// starts SendLoop/RecvLoop pumping frames for every one of them. The
// returned Client is immediately ready for OpenStream/AcceptStream.
func NewClient(addr string, partitionCount int) (*Client, error) {
	session := internal.NewSession(uint16(rand.IntN(1<<16)), uint16(rand.IntN(1<<16)), true)
	c := &Client{
		Session:       session,
		PartitionDown: make(chan PartitionDown, partitionCount),
	}

	for i := range partitionCount {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("muxshard: dial partition %d: %w", i, err)
		}

		open := protocol.Frame{Header: protocol.Header{Type: protocol.FrameSessionOpenPartition, SessionID: session.ID}}
		if _, err := protocol.WriteFrame(conn, open); err != nil {
			return nil, fmt.Errorf("muxshard: handshake on partition %d: %w", i, err)
		}

		partition := session.AddPartition(conn)

		c.sendWG.Add(1)
		go func() {
			defer c.sendWG.Done()
			session.SendLoop(conn, uint32(partition))
		}()
		go func() {
			err := session.RecvLoop(conn, uint32(partition))
			session.RemovePartition(conn)

			if c.closing.Load() {
				return
			}

			log.Printf("muxshard: session %d: partition %d down: %v", session.ID, partition, err)
			select {
			case c.PartitionDown <- PartitionDown{Partition: partition, Err: err}:
			default:
			}
		}()
	}

	return c, nil
}

// Close tears down every partition and blocks until each SendLoop has
// actually flushed its pending frames to the socket, so no data
// enqueued right before Close is silently dropped.
func (c *Client) Close() error {
	c.closing.Store(true)
	err := c.Session.Close()
	c.sendWG.Wait()
	return err
}
