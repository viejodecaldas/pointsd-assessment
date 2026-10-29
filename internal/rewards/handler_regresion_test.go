package rewards_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

func TestThreeReplicasHandleThirtyRepublishedDeliveries(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(s), newHandler(s), newHandler(s)}
	for _, reverse := range []bool{false, true} {
		var calls []func()
		for i := range 30 {
			ev := settled("payment", "member", 1200)
			if reverse {
				ev = reversed("payment", "member", 1200)
			}
			ev.EventID = fmt.Sprintf("republished_%t_%d", reverse, i)
			h := replicas[i%3]
			calls = append(calls, func() { deliver(t, h, ev) })
		}
		concurrently(calls...)
		want := int64(1200)
		if reverse {
			want = 0
		}
		if got := balance(t, s, "member"); got != want {
			t.Fatalf("reverse=%t: balance=%d, want %d", reverse, got, want)
		}
	}
}

func TestThreeReplicasAllocateMonthlyCapAcrossThirtyPayments(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(s), newHandler(s), newHandler(s)}
	var calls []func()
	for i := range 30 {
		h, ev := replicas[i%3], settled(fmt.Sprintf("payment_%d", i), "member", 700)
		calls = append(calls, func() { deliver(t, h, ev) })
	}
	concurrently(calls...)
	if got := balance(t, s, "member"); got != 10_000 {
		t.Fatalf("balance = %d, want 10000", got)
	}
}

func TestReversalUsesOriginalMemberMonthAndCappedPoints(t *testing.T) {
	s := ledger.NewMemoryStore()
	h := newHandler(s)
	deliver(t, h, settled("first", "member", 9500))
	deliver(t, h, settled("capped", "member", 1000))
	rev := reversed("capped", "different_member", 9000)
	rev.OccurredAt = time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	deliver(t, h, rev)
	deliver(t, h, settled("after_reversal", "member", 1000))
	if got := balance(t, s, "member"); got != 9500 {
		t.Fatalf("original member balance = %d, want 9500", got)
	}
	if got := balance(t, s, "different_member"); got != 0 {
		t.Fatalf("reversal payload member balance = %d, want 0", got)
	}
	used, err := s.AwardedInMonth(context.Background(), "member", "2026-10")
	if err != nil || used != 10_000 {
		t.Fatalf("October usage = %d, error = %v", used, err)
	}
}

func TestAwardTruncatesCentsAndBucketsBySettlementUTC(t *testing.T) {
	s := ledger.NewMemoryStore()
	h := newHandler(s)
	ev := settled("cents", "member", 1)
	ev.AmountCents = 12_599
	// Locally September, but October in UTC.
	ev.OccurredAt = time.Date(2026, 9, 30, 23, 30, 0, 0, time.FixedZone("UTC-5", -5*60*60))
	deliver(t, h, ev)
	if got := balance(t, s, "member"); got != 125 {
		t.Fatalf("balance = %d, want 125", got)
	}
	for month, want := range map[string]int64{"2026-09": 0, "2026-10": 125} {
		got, err := s.AwardedInMonth(context.Background(), "member", month)
		if err != nil || got != want {
			t.Fatalf("%s usage = %d, want %d; error = %v", month, got, want, err)
		}
	}
}

type failingPublisher struct{}

func (failingPublisher) Publish(context.Context, []byte, map[string]string) error {
	return errors.New("publisher unavailable")
}

func TestHandleNacksWhenDeadLetterPublicationFails(t *testing.T) {
	h := rewards.NewHandler(ledger.NewMemoryStore(), rewards.DefaultPolicy, failingPublisher{}, newHandlerLogger())
	msg := &fakeMessage{id: "invalid", data: []byte("invalid JSON"), attempt: 1}
	h.Handle(context.Background(), msg)
	if msg.outcome(t) != nacked {
		t.Fatal("failed dead-letter publication must Nack")
	}
}

func TestHandleNacksStorageFailuresAndCanceledContext(t *testing.T) {
	for _, kind := range []string{"award", "reverse"} {
		for _, canceled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/canceled=%t", kind, canceled), func(t *testing.T) {
				var fail atomic.Bool
				s := ledger.NewMemoryStore(ledger.WithFaults(func(string) error {
					if fail.Load() {
						return ledger.ErrUnavailable
					}
					return nil
				}))
				h := newHandler(s)
				ev := settled("payment", "member", 1000)
				if kind == "reverse" {
					deliver(t, h, ev)
					ev = reversed("payment", "member", 1000)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if canceled {
					cancel()
				} else {
					fail.Store(true)
				}
				msg := newMessage(t, ev)
				h.Handle(ctx, msg)
				if msg.outcome(t) != nacked {
					t.Fatal("storage failure or cancellation must Nack")
				}
				fail.Store(false)
				want := int64(0)
				if kind == "reverse" {
					want = 1000
				}
				if got := balance(t, s, "member"); got != want {
					t.Fatalf("failed operation changed balance to %d, want %d", got, want)
				}
				deliver(t, h, ev)
				if got := balance(t, s, "member"); got != 1000-want {
					t.Fatalf("redelivery balance = %d, want %d", got, 1000-want)
				}
			})
		}
	}
}

// Simulate a committed write whose successful response never reached the caller.
type lostCommitResponseStore struct {
	ledger.Store
	lostAward   atomic.Bool
	lostReverse atomic.Bool
}

func (s *lostCommitResponseStore) Award(ctx context.Context, e ledger.Entry, cap int64) error {
	if err := s.Store.Award(ctx, e, cap); err != nil {
		return err
	}
	if s.lostAward.CompareAndSwap(false, true) {
		return ledger.ErrUnavailable
	}
	return nil
}

func (s *lostCommitResponseStore) Reverse(ctx context.Context, paymentID string) error {
	if err := s.Store.Reverse(ctx, paymentID); err != nil {
		return err
	}
	if s.lostReverse.CompareAndSwap(false, true) {
		return ledger.ErrUnavailable
	}
	return nil
}

func TestHandleRedeliveryAfterLostCommitResponseIsIdempotent(t *testing.T) {
	s := &lostCommitResponseStore{Store: ledger.NewMemoryStore()}
	h := newHandler(s)
	for _, tc := range []struct {
		ev   event
		want int64
	}{
		{settled("payment", "member", 1000), 1000},
		{reversed("payment", "member", 1000), 0},
	} {
		msg := newMessage(t, tc.ev)
		h.Handle(context.Background(), msg)
		if msg.outcome(t) != nacked {
			t.Fatal("lost commit response must Nack")
		}
		deliver(t, h, tc.ev)
		if got := balance(t, s, "member"); got != tc.want {
			t.Fatalf("balance = %d, want %d", got, tc.want)
		}
	}
}

func TestHandleLogsAndAcksReversalsWithNoEarnedPoints(t *testing.T) {
	for _, zeroAward := range []bool{false, true} {
		t.Run(fmt.Sprintf("zero_award=%t", zeroAward), func(t *testing.T) {
			s := ledger.NewMemoryStore()
			var logs bytes.Buffer
			h := rewards.NewHandler(s, rewards.DefaultPolicy, &fakeDLQ{}, slog.New(slog.NewTextHandler(&logs, nil)))
			if zeroAward {
				deliver(t, h, settled("fills_cap", "member", 10_000))
				deliver(t, h, settled("payment", "member", 1000))
			}
			msg := newMessage(t, reversed("payment", "member", 1000))
			h.Handle(context.Background(), msg)
			if msg.outcome(t) != acked {
				t.Fatal("reversal without earned points must Ack")
			}
			if !strings.Contains(logs.String(), "payment_id=payment") {
				t.Fatalf("missing reconciliation log: %s", logs.String())
			}
			want := int64(0)
			if zeroAward {
				want = 10_000
			}
			if got := balance(t, s, "member"); got != want {
				t.Fatalf("balance = %d, want %d", got, want)
			}
		})
	}
}
