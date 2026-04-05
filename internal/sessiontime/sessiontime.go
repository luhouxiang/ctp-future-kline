package sessiontime

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const DefaultSessionText = "21:00-02:30,09:00-10:15,10:30-11:30,13:30-15:00"

var defaultRanges = mustParseDefaultRanges()

type Range struct {
	// Start 是交易时段起始分钟。
	Start int
	// End 是交易时段结束分钟。
	End int
}

type MinuteMeta struct {
	// GlobalSeq 是该分钟在全天交易分钟序列中的位置。
	GlobalSeq int
	// SessionID 是该分钟所属交易时段编号。
	SessionID int
	// SessionSeq 是该分钟在时段内部的顺序位置。
	SessionSeq int
}

type jsonRange struct {
	// Start 是 JSON 中的起始 HH:MM 字符串。
	Start string `json:"start"`
	// End 是 JSON 中的结束 HH:MM 字符串。
	End string `json:"end"`
}

func TradingMinuteOrderKey(minute int) int {
	if minute >= 18*60 {
		return minute
	}
	return minute + 24*60
}

func SortRangesCopy(in []Range) []Range {
	out := append([]Range(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		return TradingMinuteOrderKey(out[i].Start) < TradingMinuteOrderKey(out[j].Start)
	})
	return out
}

func ParseHHMM(v string) (int, error) {
	s := strings.TrimSpace(v)
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid HH:MM: %s", v)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}
	if h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, fmt.Errorf("invalid HH:MM: %s", v)
	}
	return h*60 + m, nil
}

func DecodeSessionJSON(raw string) ([]Range, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var src []jsonRange
	if err := json.Unmarshal([]byte(raw), &src); err != nil {
		return nil, fmt.Errorf("decode session_json failed: %w", err)
	}
	out := make([]Range, 0, len(src))
	for _, r := range src {
		start, err := ParseHHMM(r.Start)
		if err != nil {
			return nil, err
		}
		end, err := ParseHHMM(r.End)
		if err != nil {
			return nil, err
		}
		appendRange(&out, start, end)
	}
	return SortRangesCopy(out), nil
}

// SessionJSONText 把 trading_sessions.session_json 转成便于日志阅读的文本形式，
// 例如 [{"start":"21:00","end":"02:30"}] -> "21:00-02:30"。
func SessionJSONText(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	var src []jsonRange
	if err := json.Unmarshal([]byte(raw), &src); err != nil {
		return "", fmt.Errorf("decode session_json failed: %w", err)
	}
	parts := make([]string, 0, len(src))
	for _, r := range src {
		parts = append(parts, strings.TrimSpace(r.Start)+"-"+strings.TrimSpace(r.End))
	}
	return strings.Join(parts, ","), nil
}

func ParseSessionText(raw string) ([]Range, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]Range, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		se := strings.Split(item, "-")
		if len(se) != 2 {
			return nil, fmt.Errorf("invalid session range: %s", item)
		}
		start, err := ParseHHMM(se[0])
		if err != nil {
			return nil, err
		}
		end, err := ParseHHMM(se[1])
		if err != nil {
			return nil, err
		}
		appendRange(&out, start, end)
	}
	return SortRangesCopy(out), nil
}

func DefaultRanges() []Range {
	return append([]Range(nil), defaultRanges...)
}

func LabelMinute(rawMinute int, sessions []Range) (int, bool) {
	for _, s := range SortRangesCopy(sessions) {
		startEdge := s.Start
		if s.Start > 0 {
			startEdge = s.Start - 1
		}
		if rawMinute < startEdge || rawMinute > s.End {
			continue
		}
		labelStart := s.End
		if s.Start < s.End {
			labelStart = s.Start + 1
		}
		// 分钟标签使用“结束标签分钟”：
		// - 09:00:xx 这一分钟被标成 09:01
		// - 09:01:xx 被标成 09:02
		// 这样每根 1m 的 DataTime 表示该分钟结束时刻，而不是开始时刻。
		label := rawMinute + 1
		if label < labelStart {
			label = labelStart
		}
		if label > s.End {
			label = s.End
		}
		return label, true
	}
	return 0, false
}

func DistanceToTradingWindow(rawMinute int, sessions []Range) int {
	if rawMinute < 0 || rawMinute >= 24*60 || len(sessions) == 0 {
		return 24 * 60
	}
	best := 24 * 60
	for _, s := range SortRangesCopy(sessions) {
		startEdge := s.Start
		if s.Start > 0 {
			startEdge = s.Start - 1
		}
		distance := distanceToMinuteInterval(rawMinute, startEdge, s.End)
		if distance < best {
			best = distance
			if best == 0 {
				return 0
			}
		}
	}
	return best
}

func BuildLabelMinuteIndex(sessions []Range) map[int]int {
	out := make(map[int]int, 512)
	seq := 0
	for _, s := range SortRangesCopy(sessions) {
		for m := firstLabelMinute(s); m <= s.End; m += 1 {
			if _, ok := out[m]; ok {
				continue
			}
			out[m] = seq
			seq += 1
		}
	}
	return out
}

func BuildLabelMinuteMaps(sessions []Range) ([]int, map[int]MinuteMeta, map[int]int) {
	minuteOrder := make([]int, 0, 1024)
	minuteMap := make(map[int]MinuteMeta, 1024)
	sessionMinutes := make(map[int]int, len(sessions))
	global := 0
	for sid, s := range SortRangesCopy(sessions) {
		local := 0
		for m := firstLabelMinute(s); m <= s.End; m += 1 {
			if _, exists := minuteMap[m]; exists {
				continue
			}
			minuteMap[m] = MinuteMeta{GlobalSeq: global, SessionID: sid, SessionSeq: local}
			minuteOrder = append(minuteOrder, m)
			global += 1
			local += 1
		}
		// 这里统计的是“标签分钟”的数量，而不是自然钟表分钟数量。
		sessionMinutes[sid] = local
	}
	return minuteOrder, minuteMap, sessionMinutes
}

func SessionLabelStart(s Range) int {
	return firstLabelMinute(s)
}

func appendRange(out *[]Range, start int, end int) {
	if end < start {
		*out = append(*out, Range{Start: start, End: 23*60 + 59})
		*out = append(*out, Range{Start: 0, End: end})
		return
	}
	*out = append(*out, Range{Start: start, End: end})
}

func mustParseDefaultRanges() []Range {
	ranges, err := ParseSessionText(DefaultSessionText)
	if err != nil {
		panic(err)
	}
	return ranges
}

func firstLabelMinute(s Range) int {
	if s.Start < s.End {
		// 日盘场景下，session 从 09:00 开始交易，但第一根 1m 标签是 09:01。
		return s.Start + 1
	}
	// 跨午夜场景会被 appendRange 拆成两段；落到 00:00-xx 这一段时，
	// 第一根标签直接从该段结束点起算，不再额外 +1。
	return s.End
}

func distanceToMinuteInterval(minute int, start int, end int) int {
	if minute >= start && minute <= end {
		return 0
	}
	distToStart := circularMinuteDistance(minute, start)
	distToEnd := circularMinuteDistance(minute, end)
	if distToStart < distToEnd {
		return distToStart
	}
	return distToEnd
}

func circularMinuteDistance(a int, b int) int {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	if wrap := 24*60 - diff; wrap < diff {
		return wrap
	}
	return diff
}
