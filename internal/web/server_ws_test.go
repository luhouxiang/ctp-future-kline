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

func findServerConnForClient(t *testing.T, s *Server, client *websocket.Conn) *websocket.Conn {
	t.Helper()
	clientLocal := client.LocalAddr().String()
	clientRemote := client.RemoteAddr().String()
	s.mu.Lock()
	defer s.mu.Unlock()
	for serverConn := range s.wsConns {
		if serverConn == nil {
			continue
		}
		if serverConn.RemoteAddr().String() == clientLocal && serverConn.LocalAddr().String() == clientRemote {
			return serverConn
		}
	}
	return nil
}

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

	sub := quotes.ChartSubscription{Symbol: "agl9", Type: "l9", Variety: "ag", Timeframe: "1m", DataMode: "realtime"}
	key := quotes.ChartSubscriptionKey(sub)
	serverConn1 := findServerConnForClient(t, s, conn1)
	if serverConn1 == nil {
		t.Fatal("server conn1 not registered")
	}
	s.mu.Lock()
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

	sub := quotes.ChartSubscription{Symbol: "agl9", Type: "l9", Variety: "ag", Timeframe: "1m", DataMode: "realtime"}
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

	s.handleChartUnsubscribe(serverConn, []byte(`{"symbol":"agl9","type":"l9","variety":"ag","timeframe":"1m","data_mode":"realtime"}`))

	s.mu.Lock()
	_, exists := s.wsConns[serverConn].subs[key]
	s.mu.Unlock()
	if exists {
		t.Fatal("subscription still exists after unsubscribe")
	}
}

func TestBroadcastChartUpdateIsolatedByDataMode(t *testing.T) {
	s := &Server{
		status:  quotes.NewRuntimeStatusCenter(time.Minute),
		wsConns: make(map[*websocket.Conn]*wsClient),
	}
	ts := httptest.NewServer(http.HandlerFunc(s.handleWS))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	replayConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial replayConn failed: %v", err)
	}
	defer replayConn.Close()
	_, _, _ = replayConn.ReadMessage()

	realtimeConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial realtimeConn failed: %v", err)
	}
	defer realtimeConn.Close()
	_, _, _ = realtimeConn.ReadMessage()

	replaySub := quotes.ChartSubscription{Symbol: "agl9", Type: "l9", Variety: "ag", Timeframe: "1m", DataMode: "replay"}
	realtimeSub := quotes.ChartSubscription{Symbol: "agl9", Type: "l9", Variety: "ag", Timeframe: "1m", DataMode: "realtime"}
	serverReplayConn := findServerConnForClient(t, s, replayConn)
	serverRealtimeConn := findServerConnForClient(t, s, realtimeConn)
	if serverReplayConn == nil || serverRealtimeConn == nil {
		t.Fatal("server conns not registered")
	}
	replayKey := quotes.ChartSubscriptionKey(replaySub)
	realtimeKey := quotes.ChartSubscriptionKey(realtimeSub)
	s.mu.Lock()
	s.wsConns[serverReplayConn].subs[replayKey] = replaySub
	s.wsConns[serverRealtimeConn].subs[realtimeKey] = realtimeSub
	s.mu.Unlock()

	s.broadcastChartUpdate(quotes.ChartBarUpdate{
		Subscription: replaySub,
		Phase:        "partial",
		Source:       "replay",
		Bar: quotes.ChartBar{
			AdjustedTime: 100,
			DataTime:     100,
			Open:         1,
			High:         2,
			Low:          1,
			Close:        2,
		},
	})

	_ = replayConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_ = realtimeConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var msg1, msg2 map[string]any
	err1 := replayConn.ReadJSON(&msg1)
	err2 := realtimeConn.ReadJSON(&msg2)
	received := 0
	replayMessages := 0
	if err1 == nil && msg1["type"] == "chart_bar_update" {
		received++
		if sub, ok := msg1["data"].(map[string]any)["subscription"].(map[string]any); ok && sub["data_mode"] == "replay" {
			replayMessages++
		}
	}
	if err2 == nil && msg2["type"] == "chart_bar_update" {
		received++
		if sub, ok := msg2["data"].(map[string]any)["subscription"].(map[string]any); ok && sub["data_mode"] == "replay" {
			replayMessages++
		}
	}
	if received != 1 || replayMessages != 1 {
		t.Fatalf("expected exactly one replay subscription delivery, got msg1 err=%v msg=%#v msg2 err=%v msg=%#v", err1, msg1, err2, msg2)
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

	sub := quotes.ChartSubscription{Symbol: "agl9", Type: "l9", Variety: "ag", Timeframe: "1m", DataMode: "realtime"}
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

func TestBroadcastQuoteUpdateOnlyToMatchingSubscribers(t *testing.T) {
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

	sub := quotes.ChartSubscription{Symbol: "ag2605", Type: "contract", Variety: "ag", Timeframe: "5m", DataMode: "replay"}
	key := quotes.ChartSubscriptionKey(sub)
	serverConn1 := findServerConnForClient(t, s, conn1)
	if serverConn1 == nil {
		t.Fatal("server conn1 not registered")
	}
	s.mu.Lock()
	s.wsConns[serverConn1].quoteSubs[key] = sub
	s.mu.Unlock()

	price := 17111.0
	s.broadcastQuoteUpdate(quotes.ChartQuoteUpdate{
		Subscription: sub,
		Source:       "replay",
		Snapshot: quotes.ChartQuoteSnapshot{
			Symbol:      "ag2605",
			Type:        "contract",
			Variety:     "ag",
			DataMode:    "replay",
			LatestPrice: &price,
			Time:        "21:01:09",
		},
	})

	_ = conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	_ = conn2.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	var got1, got2 map[string]any
	err1 := conn1.ReadJSON(&got1)
	err2 := conn2.ReadJSON(&got2)
	received := 0
	if err1 == nil && got1["type"] == "quote_snapshot_update" {
		received++
	}
	if err2 == nil && got2["type"] == "quote_snapshot_update" {
		received++
	}
	if received != 1 {
		t.Fatalf("expected exactly one subscribed client to receive quote update, got conn1 err=%v msg=%#v conn2 err=%v msg=%#v", err1, got1, err2, got2)
	}
}
