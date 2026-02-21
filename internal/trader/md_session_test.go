package trader

import (
	"errors"
	"testing"
	"time"

	"ctp-go-demo/internal/config"
)

func TestMDSessionBackoffGrowsAndCapsWithoutJitter(t *testing.T) {
	t.Parallel()

	cfg := config.CTPConfig{
		MdReconnectInitialMS:   1000,
		MdReconnectMaxMS:       30000,
		MdReconnectJitterRatio: 0,
	}
	s := newMDSession(cfg, nil, nil, mdSessionOps{})

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 1 * time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 4, want: 8 * time.Second},
		{attempt: 5, want: 16 * time.Second},
		{attempt: 6, want: 30 * time.Second},
		{attempt: 7, want: 30 * time.Second},
	}
	for _, tc := range cases {
		if got := s.nextBackoff(tc.attempt); got != tc.want {
			t.Fatalf("nextBackoff(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
}

func TestMDSessionReconnectLoopRetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	reconnectEnabled := true
	cfg := config.CTPConfig{
		MdReconnectEnabled:     &reconnectEnabled,
		MdReconnectInitialMS:   10,
		MdReconnectMaxMS:       20,
		MdReconnectJitterRatio: 0,
		MdReloginWaitSeconds:   1,
	}
	status := NewRuntimeStatusCenter(60 * time.Second)
	attempt := 0
	loginCalls := 0
	subscribeCalls := 0
	sleeps := make([]time.Duration, 0, 8)

	s := newMDSession(cfg, status, []string{"rb2405"}, mdSessionOps{
		login: func() error {
			loginCalls++
			attempt++
			if attempt < 3 {
				return errors.New("login failed")
			}
			return nil
		},
		subscribe: func() error {
			subscribeCalls++
			return nil
		},
		sleep: func(d time.Duration) {
			sleeps = append(sleeps, d)
		},
		now: func() time.Time {
			return time.Unix(0, 0)
		},
	})

	s.reconnectLoop()

	if loginCalls != 3 {
		t.Fatalf("loginCalls = %d, want 3", loginCalls)
	}
	if subscribeCalls != 1 {
		t.Fatalf("subscribeCalls = %d, want 1", subscribeCalls)
	}
	if len(sleeps) < 4 {
		t.Fatalf("sleep calls = %d, want at least 4", len(sleeps))
	}
	if sleeps[0] != 10*time.Millisecond {
		t.Fatalf("first reconnect sleep = %s, want 10ms", sleeps[0])
	}
	if sleeps[1] != 20*time.Millisecond {
		t.Fatalf("second reconnect sleep = %s, want 20ms", sleeps[1])
	}
	if sleeps[2] != 20*time.Millisecond {
		t.Fatalf("third reconnect sleep = %s, want 20ms", sleeps[2])
	}
	if sleeps[len(sleeps)-1] != time.Second {
		t.Fatalf("last sleep (relogin wait) = %s, want 1s", sleeps[len(sleeps)-1])
	}
	snap := status.Snapshot(time.Now())
	if snap.MdReconnectTry != 0 {
		t.Fatalf("MdReconnectTry = %d, want 0 after success", snap.MdReconnectTry)
	}
	if !snap.MdSubscribed || snap.SubscribeCount != 1 {
		t.Fatalf("subscription status unexpected: %+v", snap)
	}
}
