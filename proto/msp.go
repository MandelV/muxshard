package proto

//Multiplexed Sharded Protocol

type Session struct {
	ID         uint16
	RoutinSeed uint16
	Partition  uint16
}

// client streams = IDs impairs
// server streams = IDs pairs
type Header struct {
	Type     uint8
	Flags    uint8
	StreamID uint16
	Length   uint64
	Reserved uint16
}
