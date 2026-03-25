package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ctp-future-kline/internal/quotes"

	"github.com/gorilla/websocket"
)

func TestBroadcastChartUpdateOnlyToMatchingSubscribers(t *testing.T) {
	s := &Server{
		status:  quotes.NewRuntimeStatusCenter(time.Minute),
		wsConns: make(map[*websocket.Conn]*wsClient),
	}
	ts := httptest.NewServer(http.HandlerFunc(s.handleWS))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	conn1, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial conn1 failed: %v", err)
	}
	defer conn1.Close()
	_, _, _ = conn1.ReadMessage()

	conn2, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial conn2 failed: %v", err)
	}
	defer conn2.Close()
	_, _, _ = conn2.ReadMessage()

	sub := quotes.ChartSubscription{Symbol: "agl9", Type: "l9", Variety: "ag", Timeframe: "1m"}
	key := quotes.ChartSubscriptionKey(sub)
	var serverConn1 *websocket.Conn
	s.mu.Lock()
	for conn := range s.wsConns {
		serverConn1 = conn
		break
	}
	if serverConn1 == nil {
		s.mu.Unlock()
		t.Fatal("server conn1 not registered")
	}
	s.wsConns[serverConn1].subs[key] = sub
	s.mu.Unlock()

	s.broadcastChartUpdate(quotes.ChartBarUpdate{
		Subscription: sub,
		Phase:        "partial",
		Source:       "realtime",
		Bar: quotes.ChartBar{
			AdjustedTime: 100,
			DataTime:     100,
			Open:         1,
			High:         2,
			Low:          1,
			Close:        2,
		},
	})

	_ = conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	_ = conn2.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	var got1, got2 map[string]any
	err1 := conn1.ReadJSON(&got1)
	err2 := conn2.ReadJSON(&got2)
	received := 0
	if err1 == nil && got1["type"] == "chart_bar_update" {
		received += 1
	}
	if err2 == nil && got2["type"] == "chart_bar_update" {
		received += 1
	}
	if received != 1 {
		t.Fatalf("expected exactly one subscribed client to receive update, got conn1 err=%v msg=%#v conn2 err=%v msg=%#v", err1, got1, err2, got2)
	}
}

func TestHandleChartUnsubscribeRemovesLocalSubscription(t *testing.T) {
	s := &Server{
		status:  quotes.NewRuntimeStatusCenter(time.Minute),
		wsConns: make(map[*websocket.Conn]*wsClient),
	}
	ts := httptest.NewServer(http.HandlerFunc(s.handleWS))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	_, _, _ = conn.ReadMessage()

	sub := quotes.ChartSubscription{Symbol: "agl9", Type: "l9", Variety: "ag", Timeframe: "1m"}
	key := quotes.ChartSubscriptionKey(sub)
	var serverConn *websocket.Conn
	s.mu.Lock()
	for item := range s.wsConns {
		serverConn = item
		break
	}
	if serverConn == nil {
		s.mu.Unlock()
		t.Fatal("server conn not registered")
	}
	s.wsConns[serverConn].subs[key] = sub
	s.mu.Unlock()

	s.handleChartUnsubscribe(serverConn, []byte(`{"symbol":"agl9","type":"l9","variety":"ag","timeframe":"1m"}`))

	s.mu.Lock()
	_, exists := s.wsConns[serverConn].subs[key]
	s.mu.Unlock()
	if exists {
		t.Fatal("subscription still exists after unsubscribe")
	}
}

func TestBroadcastEventDoesNotBlockChartBroadcast(t *testing.T) {
	s := &Server{
		status:  quotes.NewRuntimeStatusCenter(time.Minute),
		wsConns: make(map[*websocket.Conn]*wsClient),
	}
	ts := httptest.NewServer(http.HandlerFunc(s.handleWS))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	_, _, _ = conn.ReadMessage()

	sub := quotes.ChartSubscription{Symbol: "agl9", Type: "l9", Variety: "ag", Timeframe: "1m"}
	key := quotes.ChartSubscriptionKey(sub)
	var serverConn *websocket.Conn
	s.mu.Lock()
	for item := range s.wsConns {
		serverConn = item
		break
	}
	if serverConn == nil {
		s.mu.Unlock()
		t.Fatal("server conn not registered")
	}
	s.wsConns[serverConn].subs[key] = sub
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.broadcastEvent("status_update", map[string]any{"ok": true})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcastEvent blocked")
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var statusMsg map[string]any
	if err := conn.ReadJSON(&statusMsg); err != nil {
		t.Fatalf("read status_update failed: %v", err)
	}
	if statusMsg["type"] != "status_update" {
		t.Fatalf("unexpected first message: %#v", statusMsg)
	}

	done = make(chan struct{})
	go func() {
		s.broadcastChartUpdate(quotes.ChartBarUpdate{
			Subscription: sub,
			Phase:        "partial",
			Source:       "realtime",
			Bar: quotes.ChartBar{
				AdjustedTime: 101,
				DataTime:     101,
				Open:         1,
				High:         2,
				Low:          1,
				Close:        2,
			},
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcastChartUpdate blocked after broadcastEvent")
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var chartMsg map[string]any
	if err := conn.ReadJSON(&chartMsg); err != nil {
		t.Fatalf("read chart_bar_update failed: %v", err)
	}
	if chartMsg["type"] != "chart_bar_update" {
		t.Fatalf("unexpected second message: %#v", chartMsg)
	}
}
