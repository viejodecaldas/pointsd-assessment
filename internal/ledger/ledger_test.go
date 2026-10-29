package ledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func runConcurrent(t *testing.T, n int, operation func(int) error) {
	t.Helper()
	start := make(chan struct{})
	var done sync.WaitGroup
	for i := range n {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			<-start
			if err := operation(i); err != nil {
				t.Errorf("operation %d: %v", i, err)
			}
		}(i)
	}
	close(start)
	done.Wait()
}

func TestAtomicAwardStoresOneEntryForConcurrentDuplicates(t *testing.T) {
	s := NewMemoryStore(WithLatency(time.Millisecond))
	e := Entry{MemberID: "member", PaymentID: "payment", Points: 2000, Month: "2026-10"}
	runConcurrent(t, 30, func(int) error { return s.Award(context.Background(), e, 10_000) })
	if len(s.entries) != 1 {
		t.Fatalf("stored %d entries, want 1", len(s.entries))
	}
	e.Kind = KindAward
	if s.entries[0] != e {
		t.Fatalf("entry = %+v, want %+v", s.entries[0], e)
	}
}

func TestAtomicAwardStoresCapCutsAndZeroEntries(t *testing.T) {
	s := NewMemoryStore(WithLatency(time.Millisecond))
	runConcurrent(t, 30, func(i int) error {
		return s.Award(context.Background(), Entry{
			MemberID: "member", PaymentID: fmt.Sprintf("payment_%d", i), Points: 700, Month: "2026-10",
		}, 10_000)
	})
	if len(s.entries) != 30 {
		t.Fatalf("stored %d entries, want 30 including zero awards", len(s.entries))
	}
	counts := make(map[int64]int)
	seen := make(map[string]bool)
	var total int64
	for _, e := range s.entries {
		if e.Kind != KindAward || seen[e.PaymentID] {
			t.Fatalf("invalid or duplicate entry: %+v", e)
		}
		seen[e.PaymentID] = true
		counts[e.Points]++
		total += e.Points
	}
	if total != 10_000 || counts[700] != 14 || counts[200] != 1 || counts[0] != 15 {
		t.Fatalf("stored total = %d, point counts = %v", total, counts)
	}
}

func TestAtomicReverseUsesOriginalEntryAndDoesNotRestoreCap(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore(WithLatency(time.Millisecond))
	for _, e := range []Entry{
		{MemberID: "member", PaymentID: "first", Points: 9500, Month: "2026-10"},
		{MemberID: "member", PaymentID: "capped", Points: 1000, Month: "2026-10"},
	} {
		if err := s.Award(ctx, e, 10_000); err != nil {
			t.Fatal(err)
		}
	}
	runConcurrent(t, 30, func(int) error { return s.Reverse(ctx, "capped") })
	want := Entry{MemberID: "member", PaymentID: "capped", Kind: KindReversal, Points: -500, Month: "2026-10"}
	if len(s.entries) != 3 || s.entries[2] != want {
		t.Fatalf("stored entries = %+v, want one reversal %+v", s.entries, want)
	}
	zero := Entry{MemberID: "member", PaymentID: "zero", Points: 1000, Month: "2026-10"}
	if err := s.Award(ctx, zero, 10_000); err != nil {
		t.Fatal(err)
	}
	if s.entries[3].Points != 0 {
		t.Fatalf("award after reversal = %+v, want zero", s.entries[3])
	}
	// Even a changed month/member in a republished payment cannot earn again.
	zero.Month, zero.MemberID = "2026-11", "different_member"
	if err := s.Award(ctx, zero, 10_000); err != nil {
		t.Fatal(err)
	}
	if err := s.Reverse(ctx, "zero"); !errors.Is(err, ErrNoPointsAwarded) {
		t.Fatalf("zero award reversal error = %v", err)
	}
	if err := s.Reverse(ctx, "missing"); !errors.Is(err, ErrUnknownPayment) {
		t.Fatalf("missing award reversal error = %v", err)
	}
	if len(s.entries) != 4 {
		t.Fatalf("stored %d entries, want 4", len(s.entries))
	}
	used, err := s.AwardedInMonth(ctx, "member", "2026-10")
	if err != nil || used != 10_000 {
		t.Fatalf("monthly usage = %d, error = %v", used, err)
	}
}

func TestAtomicOperationsFailuresHaveNoEffect(t *testing.T) {
	for _, operation := range []string{"award", "reverse"} {
		t.Run(operation, func(t *testing.T) {
			var fail atomic.Bool
			s := NewMemoryStore(WithFaults(func(op string) error {
				if op == operation && fail.Load() {
					return ErrUnavailable
				}
				return nil
			}))
			ctx := context.Background()
			e := Entry{MemberID: "member", PaymentID: "payment", Points: 1000, Month: "2026-10"}
			call := func() error { return s.Award(ctx, e, 10_000) }
			if operation == "reverse" {
				if err := call(); err != nil {
					t.Fatal(err)
				}
				call = func() error { return s.Reverse(ctx, e.PaymentID) }
			}
			before := len(s.entries)
			fail.Store(true)
			if err := call(); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("error = %v, want unavailable", err)
			}
			if len(s.entries) != before {
				t.Fatal("failed operation changed the ledger")
			}
			fail.Store(false)
			if err := call(); err != nil {
				t.Fatal(err)
			}
			if len(s.entries) != before+1 {
				t.Fatal("retry did not store exactly one entry")
			}
		})
	}
}

func TestAtomicOperationsRespectCanceledContext(t *testing.T) {
	for _, latency := range []time.Duration{0, time.Hour} {
		t.Run(latency.String(), func(t *testing.T) {
			s := NewMemoryStore(WithLatency(latency))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := s.Award(ctx, Entry{PaymentID: "payment", Points: 100}, 10_000); !errors.Is(err, context.Canceled) {
				t.Fatalf("award error = %v", err)
			}
			if err := s.Reverse(ctx, "payment"); !errors.Is(err, context.Canceled) {
				t.Fatalf("reverse error = %v", err)
			}
			if len(s.entries) != 0 {
				t.Fatal("canceled operations wrote entries")
			}
		})
	}
}
