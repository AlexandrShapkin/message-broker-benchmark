package workload

import (
	"context"
	"math"
	"time"
)

// Schedule uses absolute deadlines; emit must be nonblocking. Returned count
// includes all scheduled arrivals, including arrivals rejected by emit.
func Schedule(ctx context.Context, start time.Time, duration time.Duration, rate float64, burst float64, period, on time.Duration, emit func(time.Time)) uint64 {
	if rate == 0 {
		timer := time.NewTimer(time.Until(start.Add(duration)))
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		return 0
	}
	if burst < 1 {
		burst = 1
	}
	end := start.Add(duration)
	var n uint64
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		deadline := start.Add(arrival(n, rate, burst, period, on))
		if !deadline.Before(end) {
			return n
		}
		if ctx.Err() != nil {
			return n
		}
		if wait := time.Until(deadline); wait > 0 {
			timer.Reset(wait)
			select {
			case <-ctx.Done():
				return n
			case <-timer.C:
			}
		}
		if time.Now().After(end) {
			return n
		}
		emit(deadline)
		n++
	}
}

// arrival inverts the integrated requested rate. Computing each deadline from
// its index avoids accumulated nanosecond truncation and skipped burst windows
// when a low base rate has an interarrival longer than a burst period.
func arrival(index uint64, rate, burst float64, period, on time.Duration) time.Duration {
	count := float64(index)
	seconds := count / rate
	if burst > 1 && period > 0 {
		cycle := rate * (period.Seconds() + (burst-1)*on.Seconds())
		cycles := math.Floor(count / cycle)
		rest := count - cycles*cycle
		burstCount := rate * burst * on.Seconds()
		seconds = cycles * period.Seconds()
		if rest < burstCount {
			seconds += rest / (rate * burst)
		} else {
			seconds += on.Seconds() + (rest-burstCount)/rate
		}
	}
	nanos := seconds * float64(time.Second)
	if nanos >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(math.Round(nanos))
}
func Requested(duration time.Duration, rate, burst float64, period, on time.Duration) float64 {
	if burst <= 1 || period <= 0 {
		return duration.Seconds() * rate
	}
	active := (duration / period) * on
	rest := duration % period
	if rest > on {
		rest = on
	}
	active += rest
	return rate * (duration.Seconds() + float64(burst-1)*active.Seconds())
}
