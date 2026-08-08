package internal

import "testing"

func TestScoreDeterministic(t *testing.T) {
	const seed, streamID, partitionCount = 42, 1337, 16

	want := Shard(seed, streamID, partitionCount)
	for i := 0; i < 100; i++ {
		if got := Shard(seed, streamID, partitionCount); got != want {
			t.Fatalf("Score is not deterministic: got %d, want %d", got, want)
		}
	}
}

func TestScoreWithinPartitionRange(t *testing.T) {
	const partitionCount = 8

	for seed := uint64(0); seed < 50; seed++ {
		for streamID := uint64(0); streamID < 50; streamID++ {
			if got := Shard(seed, streamID, partitionCount); got >= partitionCount {
				t.Fatalf("Score(%d, %d, %d) = %d, out of range [0, %d)", seed, streamID, partitionCount, got, partitionCount)
			}
		}
	}
}

func TestScoreZeroPartitionCount(t *testing.T) {
	if got := Shard(1, 2, 0); got != 0 {
		t.Fatalf("Score with partitionCount=0 = %d, want 0", got)
	}
}

func TestScoreDistribution(t *testing.T) {
	const partitionCount = 4
	const streamCount = 4000

	counts := make(map[uint64]int)
	for streamID := uint64(0); streamID < streamCount; streamID++ {
		counts[Shard(1, streamID, partitionCount)]++
	}

	if len(counts) != partitionCount {
		t.Fatalf("only %d/%d partitions were used: %v", len(counts), partitionCount, counts)
	}

	expected := streamCount / partitionCount
	for partition, count := range counts {
		if deviation := abs(count-expected) * 100 / expected; deviation > 20 {
			t.Errorf("partition %d got %d streams, expected ~%d (deviation %d%%)", partition, count, expected, deviation)
		}
	}
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
