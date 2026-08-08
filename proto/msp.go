package proto

// Multiplexed Sharded Protocol
type FrameType = uint8

const (
	FrameSessionOpen  FrameType = 0x01
	FrameData         FrameType = 0x02
	FrameFin          FrameType = 0x03
	FrameRST          FrameType = 0x04
	FrameWindowUpdate FrameType = 0x05

	FramePing FrameType = 0x10
	FramePong FrameType = 0x11

	FrameGoAway FrameType = 0x12
)

// client streams = IDs impairs
// server streams = IDs pairs
type Header struct {
	Type      FrameType
	Flags     uint8
	SessionID uint16
	StreamID  uint16
	Length    uint64
	Reserved  uint16
}

type Frame struct {
	Header Header
	Data   []byte
}

