package quotes

import (
	"testing"
	"time"
)

func TestDedupePersistTasksKeepsLatestValuePerPrimaryKey(t *testing.T) {
	t.Parallel()

	minute := mustParseWriterTestTime(t, "2026-04-09 09:31:00")
	tasks := []persistTask{
		{
			TableName:    "future_kline_instrument_1m_rb",
			InstrumentID: "rb2501",
			Bar: minuteBar{
				InstrumentID: "rb2501",
				Period:       "1m",
				MinuteTime:   minute,
				Close:        101,
			},
		},
		{
			TableName:    "future_kline_instrument_1m_rb",
			InstrumentID: "rb2501",
			Bar: minuteBar{
				InstrumentID: "rb2501",
				Period:       "1m",
				MinuteTime:   minute,
				Close:        102,
			},
		},
		{
			TableName:    "future_kline_instrument_1m_rb",
			InstrumentID: "rb2505",
			Bar: minuteBar{
				InstrumentID: "rb2505",
				Period:       "1m",
				MinuteTime:   minute,
				Close:        201,
			},
		},
	}

	got := dedupePersistTasks(tasks)
	if len(got) != 2 {
		t.Fatalf("len(dedupePersistTasks()) = %d, want 2", len(got))
	}

	if got[0].InstrumentID != "rb2501" || got[0].Bar.Close != 102 {
		t.Fatalf("first deduped task = %+v, want latest rb2501 close=102", got[0])
	}
	if got[1].InstrumentID != "rb2505" || got[1].Bar.Close != 201 {
		t.Fatalf("second deduped task = %+v, want rb2505 close=201", got[1])
	}
}

func mustParseWriterTestTime(t *testing.T, text string) time.Time {
	t.Helper()
	v, err := time.ParseInLocation("2006-01-02 15:04:05", text, time.Local)
	if err != nil {
		t.Fatalf("time.ParseInLocation(%q) error = %v", text, err)
	}
	return v
}
