package metrics

import (
	"encoding/binary"
	"fmt"
	hdr "github.com/HdrHistogram/hdrhistogram-go"
	"hash/crc32"
	"sync"
	"time"
)

const Header = 64

func Encode(size int, token [16]byte, producer uint32, seq uint64, deadline int64, phase uint32) []byte {
	b := make([]byte, size)
	copy(b[:4], "MBB1")
	copy(b[4:20], token[:])
	binary.LittleEndian.PutUint32(b[20:24], producer)
	binary.LittleEndian.PutUint64(b[24:32], seq)
	binary.LittleEndian.PutUint64(b[32:40], uint64(deadline))
	binary.LittleEndian.PutUint32(b[40:44], phase)
	for i := 44; i < size; i++ {
		b[i] = byte(uint64(i)*31 + seq + uint64(producer))
	}
	checksum := crc32.Update(0, crc32.IEEETable, b[:44])
	checksum = crc32.Update(checksum, crc32.IEEETable, b[48:])
	binary.LittleEndian.PutUint32(b[44:48], checksum)
	return b
}
func Decode(b []byte, token [16]byte) (uint32, uint64, int64, uint32, error) {
	if len(b) < Header || string(b[:4]) != "MBB1" || string(b[4:20]) != string(token[:]) {
		return 0, 0, 0, 0, fmt.Errorf("invalid benchmark envelope")
	}
	checksum := crc32.Update(0, crc32.IEEETable, b[:44])
	checksum = crc32.Update(checksum, crc32.IEEETable, b[48:])
	phase := binary.LittleEndian.Uint32(b[40:44])
	deadline := int64(binary.LittleEndian.Uint64(b[32:40]))
	if checksum != binary.LittleEndian.Uint32(b[44:48]) || binary.LittleEndian.Uint64(b[24:32]) == 0 || (phase != 1 && phase != 2) || deadline < 0 {
		return 0, 0, 0, 0, fmt.Errorf("corrupt benchmark envelope")
	}
	return binary.LittleEndian.Uint32(b[20:24]), binary.LittleEndian.Uint64(b[24:32]), deadline, phase, nil
}

type interval struct{ lo, hi uint64 }
type Sequence struct {
	ranges []interval
	max    uint64
}

// Add stores sorted disjoint intervals; storage follows fragmentation, not count.
func (s *Sequence) Add(n uint64, limit int) (duplicate, reordered bool, err error) {
	if n == 0 {
		return false, false, fmt.Errorf("zero sequence")
	}
	reordered = n < s.max
	for i, r := range s.ranges {
		if n >= r.lo && n <= r.hi {
			return true, false, nil
		}
		if n < r.lo {
			if n+1 == r.lo {
				s.ranges[i].lo = n
				if i > 0 && s.ranges[i-1].hi+1 == n {
					s.ranges[i-1].hi = r.hi
					s.ranges = append(s.ranges[:i], s.ranges[i+1:]...)
				}
			} else if i > 0 && s.ranges[i-1].hi+1 == n {
				s.ranges[i-1].hi = n
			} else {
				if len(s.ranges) >= limit {
					return false, false, fmt.Errorf("correctness interval limit exceeded")
				}
				s.ranges = append(s.ranges, interval{})
				copy(s.ranges[i+1:], s.ranges[i:])
				s.ranges[i] = interval{n, n}
			}
			return false, reordered, nil
		}
	}
	if len(s.ranges) > 0 && s.ranges[len(s.ranges)-1].hi+1 == n {
		s.ranges[len(s.ranges)-1].hi = n
	} else {
		if len(s.ranges) >= limit {
			return false, false, fmt.Errorf("correctness interval limit exceeded")
		}
		s.ranges = append(s.ranges, interval{n, n})
	}
	if n > s.max {
		s.max = n
	}
	return false, reordered, nil
}

type Recorder struct {
	Mu           sync.Mutex
	Counters     map[string]uint64
	Hists        map[string]*hdr.Histogram
	window       *hdr.Histogram
	intervals    int
	sequences    map[[2]int]*Sequence
	attempted    map[int]uint64
	MaxIntervals int
	Invalid      string
}

func New(limit int) *Recorder {
	r := &Recorder{Counters: map[string]uint64{}, Hists: map[string]*hdr.Histogram{}, sequences: map[[2]int]*Sequence{}, attempted: map[int]uint64{}, MaxIntervals: limit}
	for _, k := range []string{"sent", "producer_accepted", "producer_accepted_window", "delivered", "delivered_window", "unique_delivered_window", "consumer_acked", "lost", "duplicate", "unexpected", "reordered", "errors", "disconnects", "scheduled", "queue_dropped", "unique_delivered", "connections_established", "connections_failed", "reconnections"} {
		r.Counters[k] = 0
	}
	for _, k := range []string{"publish", "delivery", "scheduling_lag", "connect"} {
		r.Hists[k] = hdr.New(1, int64(24*time.Hour/time.Microsecond), 3)
	}
	r.window = hdr.New(1, int64(24*time.Hour/time.Microsecond), 3)
	return r
}
func (r *Recorder) Inc(k string, n uint64) { r.Mu.Lock(); defer r.Mu.Unlock(); r.Counters[k] += n }

// Attempt records the contiguous sequence prefix immediately before Publish.
// It costs one integer per producer, independently of total message count.
func (r *Recorder) Attempt(producer int, seq uint64) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	if seq != r.attempted[producer]+1 {
		r.Invalid = "non-contiguous producer attempt sequence"
		return
	}
	r.attempted[producer] = seq
	r.Counters["sent"]++
}

func (r *Recorder) Expected(producer int, seq uint64) bool {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return seq > 0 && seq <= r.attempted[producer]
}
func (r *Recorder) Record(k string, d time.Duration) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	v := d.Microseconds()
	if v < 1 {
		v = 1
	}
	if e := r.Hists[k].RecordValue(v); e != nil {
		r.Invalid = e.Error()
	}
	if k == "delivery" {
		if e := r.window.RecordValue(v); e != nil {
			r.Invalid = e.Error()
		}
	}
}
func (r *Recorder) Delivery(consumer, producer int, seq uint64, ordered bool) bool {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.Counters["delivered"]++
	if r.Invalid != "" {
		return false
	}
	key := [2]int{consumer, producer}
	s := r.sequences[key]
	if s == nil {
		s = &Sequence{}
		r.sequences[key] = s
	}
	before := len(s.ranges)
	dup, reorder, e := s.Add(seq, r.MaxIntervals)
	r.intervals += len(s.ranges) - before
	if r.intervals > r.MaxIntervals {
		r.Invalid = "global correctness interval limit exceeded"
		return false
	}
	if e != nil {
		r.Invalid = e.Error()
		return false
	}
	if dup {
		r.Counters["duplicate"]++
	} else {
		r.Counters["unique_delivered"]++
	}
	if reorder && ordered {
		r.Counters["reordered"]++
	}
	return !dup
}
func (r *Recorder) Snapshot() map[string]uint64 {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	m := map[string]uint64{}
	for k, v := range r.Counters {
		m[k] = v
	}
	return m
}
func (r *Recorder) Histogram(k string) map[string]any {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	h := r.Hists[k]
	return map[string]any{"schema_version": 1, "encoding": "hdr-go-snapshot-v1", "unit": "microseconds", "snapshot": h.Export(), "distribution": h.Distribution(), "count": h.TotalCount()}
}

// Window preserves sparse HDR buckets for latency-versus-time reconstruction.
func (r *Recorder) Window() map[string]any {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	bars := []hdr.Bar{}
	for _, b := range r.window.Distribution() {
		if b.Count > 0 {
			bars = append(bars, b)
		}
	}
	out := map[string]any{"unit": "microseconds", "encoding": "hdr-buckets-v1", "count": r.window.TotalCount(), "distribution": bars}
	r.window.Reset()
	return out
}
