//go:build js && wasm

package main

import (
	"encoding/json"
	"syscall/js"
	"time"

	"ctp-future-kline/internal/klinecompose"
	"ctp-future-kline/internal/sessiontime"
)

type wasmState struct {
	ticketID string
	sessions []sessiontime.Range
	bars1m   []klinecompose.Bar
	latest   map[string]klinecompose.Bar
}

var state = &wasmState{
	latest: make(map[string]klinecompose.Bar),
}

func main() {
	js.Global().Set("klinecompose_init", js.FuncOf(initComposer))
	js.Global().Set("klinecompose_push_tick", js.FuncOf(pushTick))
	js.Global().Set("klinecompose_get_bar", js.FuncOf(getBar))
	js.Global().Set("klinecompose_get_series", js.FuncOf(getSeries))
	js.Global().Set("klinecompose_reset", js.FuncOf(resetComposer))
	select {}
}

func initComposer(_ js.Value, args []js.Value) any {
	if len(args) < 1 {
		return false
	}
	var payload struct {
		TicketID string `json:"ticket_id"`
		Sessions []struct {
			Start int `json:"start"`
			End   int `json:"end"`
		} `json:"sessions"`
	}
	if err := json.Unmarshal([]byte(args[0].String()), &payload); err != nil {
		return false
	}
	state.ticketID = payload.TicketID
	state.sessions = state.sessions[:0]
	state.bars1m = state.bars1m[:0]
	state.latest = make(map[string]klinecompose.Bar)
	for _, s := range payload.Sessions {
		state.sessions = append(state.sessions, sessiontime.Range{Start: s.Start, End: s.End})
	}
	return true
}

func pushTick(_ js.Value, args []js.Value) any {
	if len(args) < 1 {
		return js.Null()
	}
	var tick klinecompose.Tick
	if err := json.Unmarshal([]byte(args[0].String()), &tick); err != nil {
		return js.Null()
	}
	var prev *klinecompose.Bar
	if len(state.bars1m) > 0 {
		prev = &state.bars1m[len(state.bars1m)-1]
	}
	bar, ok := klinecompose.TickTo1M(time.Now(), tick, state.sessions, prev)
	if !ok {
		return js.Null()
	}
	if len(state.bars1m) > 0 && state.bars1m[len(state.bars1m)-1].AdjustedTime == bar.AdjustedTime {
		state.bars1m[len(state.bars1m)-1] = bar
	} else {
		state.bars1m = append(state.bars1m, bar)
	}
	state.latest["1m"] = bar
	raw, _ := json.Marshal(bar)
	return string(raw)
}

func getBar(_ js.Value, args []js.Value) any {
	if len(args) < 1 {
		return js.Null()
	}
	tf := args[0].String()
	if tf == "1m" {
		bar, ok := state.latest["1m"]
		if !ok {
			return js.Null()
		}
		raw, _ := json.Marshal(bar)
		return string(raw)
	}
	series := klinecompose.AggregateFrom1M(state.bars1m, tf)
	if len(series) == 0 {
		return js.Null()
	}
	raw, _ := json.Marshal(series[len(series)-1])
	return string(raw)
}

func getSeries(_ js.Value, args []js.Value) any {
	if len(args) < 1 {
		return "[]"
	}
	tf := args[0].String()
	if tf == "1m" {
		raw, _ := json.Marshal(state.bars1m)
		return string(raw)
	}
	raw, _ := json.Marshal(klinecompose.AggregateFrom1M(state.bars1m, tf))
	return string(raw)
}

func resetComposer(_ js.Value, args []js.Value) any {
	if len(args) > 0 {
		id := args[0].String()
		if id != "" && id != state.ticketID {
			return true
		}
	}
	state.ticketID = ""
	state.sessions = nil
	state.bars1m = nil
	state.latest = make(map[string]klinecompose.Bar)
	return true
}
