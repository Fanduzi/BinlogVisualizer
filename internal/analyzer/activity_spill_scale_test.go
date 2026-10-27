package analyzer

import (
	"fmt"
	"math"
	"testing"
	"time"
)

// Sequential points, 500 tables a minute. A flat merge of every run makes
// io/file grow with the point count. Tiered merging keeps that ratio logarithmic
// and the wall time near-linear across 0.25M, 0.5M, and 1M points.
func TestSequentialSpillIOScales(t *testing.T) {
	sizes := []int{250_000, 500_000, 1_000_000}
	var first, prev seqMeasure
	for _, n := range sizes {
		m := measureSequentialFlat(t, n)
		ratio := float64(m.io) / float64(m.file)
		t.Logf("seq points=%d wall=%s io=%d file=%d peak=%d ratio=%.2f", m.points, m.wall, m.io, m.file, m.peak, ratio)
		if lim := flatIOLimit(m.points, m.file); m.io > lim {
			t.Fatalf("%d points: spill io %d exceeds %d (%.2f× file)", m.points, m.io, lim, ratio)
		}
		if prev.points > 0 && ratio > prev.ratio*1.35 && ratio > prev.ratio+8 {
			t.Fatalf("io/file %.2f at %d points vs %.2f at %d, want logarithmic", ratio, m.points, prev.ratio, prev.points)
		}
		if first.points > 0 && m.points >= first.points*4 && m.wall > first.wall*9/2+time.Second {
			t.Fatalf("%d points took %s vs %s at %d, want at most about 4.5×", m.points, m.wall, first.wall, first.points)
		}
		m.ratio = ratio
		if first.points == 0 {
			first = m
		}
		prev = m
	}
}

type seqMeasure struct {
	points int
	wall   time.Duration
	io     int64
	file   int64
	peak   int64
	ratio  float64
}

func flatIOLimit(points int, file int64) int64 {
	const coeff = 8
	cells := spillResidentBytes / actRecSize
	if cells < 1 {
		cells = 1
	}
	ratio := float64(points) / float64(cells)
	if ratio < 1 {
		ratio = 1
	}
	logs := math.Log(ratio) / math.Log(float64(spillRunK))
	return int64(math.Ceil(float64(coeff) * float64(file) * (1 + logs)))
}

func measureSequentialFlat(t *testing.T, points int) seqMeasure {
	t.Helper()
	s := newActivityStore()
	s.limit = 0
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	const tables = 500
	start := time.Now()
	for i := 0; i < points; i++ {
		minute := base.Add(time.Duration(i/tables) * time.Minute)
		id := tableIdentity{schema: "s", table: fmt.Sprintf("t%03d", i%tables)}
		s.addRows(id, minute, 1)
	}
	s.flushAll()
	wall := time.Since(start)
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if s.n != points {
		t.Fatalf("points = %d, want %d", s.n, points)
	}
	return seqMeasure{points: s.n, wall: wall, io: s.spillIO(), file: s.spillBytes(), peak: s.peakSpill()}
}
