package internal

func Score(seed, StreamID, partitionCount uint64) uint64 {
	if partitionCount == 0 {
		return 0
	}

	h := seed + 0x9e3779b97f4a7c15 + StreamID
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33

	return h % partitionCount
}
