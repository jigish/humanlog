package humanlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/fatih/color"
)

// JournalJSONHandler can handle logs emmited by logrus.TextFormatter loggers.
type JournalJSONHandler struct {
	buf     *bytes.Buffer
	out     *tabwriter.Writer
	truncKV int

	Opts *HandlerOptions

	Level   string
	Time    time.Time
	Message string
	Fields  map[string]string

	last map[string]string
}

func (h *JournalJSONHandler) clear() {
	h.Level = ""
	h.Time = time.Time{}
	h.Message = ""
	h.last = h.Fields
	h.Fields = make(map[string]string)
	if h.buf != nil {
		h.buf.Reset()
	}
}

// TryHandle tells if this line was handled by this handler.
func (h *JournalJSONHandler) TryHandle(d []byte) bool {
	if !bytes.Contains(d, []byte(`"_SOURCE_REALTIME_TIMESTAMP"`)) {
		return false
	}
	err := h.UnmarshalJournalJSON(d)
	if err != nil {
		h.clear()
		return false
	}
	return true
}

// UnmarshalJournalJSON sets the fields of the handler.
func (h *JournalJSONHandler) UnmarshalJournalJSON(data []byte) error {
	raw := make(map[string]interface{})
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	timestamp, ok := raw["_SOURCE_REALTIME_TIMESTAMP"]
	if ok {
		delete(raw, "_SOURCE_REALTIME_TIMESTAMP")
		timeString, ok := timestamp.(string)
		if !ok {
			return fmt.Errorf("_SOURCE_REALTIME_TIMESTAMP %v is not type string", timestamp)
		}
		timeMicros, err := strconv.ParseInt(timeString, 10, 64)
		if err != nil {
			return err
		}
		h.Time = time.Unix(timeMicros/int64(1e6), (timeMicros%int64(1e6))*int64(1e3))
	}
	if h.Message, ok = raw["MESSAGE"].(string); ok {
		delete(raw, "MESSAGE")
	}

	if h.Level, ok = raw["PRIORITY"].(string); ok {
		delete(raw, "PRIORITY")
	}

	if h.Fields == nil {
		h.Fields = make(map[string]string)
	}

	for key, val := range raw {
		switch v := val.(type) {
		case float64:
			if v-math.Floor(v) < 0.000001 && v < 1e9 {
				// looks like an integer that's not too large
				h.Fields[key] = fmt.Sprintf("%d", int(v))
			} else {
				h.Fields[key] = fmt.Sprintf("%g", v)
			}
		case string:
			h.Fields[key] = fmt.Sprintf("%q", v)
		default:
			h.Fields[key] = fmt.Sprintf("%v", v)
		}
	}

	return nil
}

// Prettify the output in a logrus like fashion.
func (h *JournalJSONHandler) Prettify(skipUnchanged bool) []byte {
	defer h.clear()
	if h.out == nil {
		if h.Opts == nil {
			h.Opts = DefaultOptions
		}
		h.buf = bytes.NewBuffer(nil)
		h.out = tabwriter.NewWriter(h.buf, 0, 1, 0, '\t', 0)
	}

	var (
		msgColor       *color.Color
		msgAbsentColor *color.Color
	)
	if h.Opts.LightBg {
		msgColor = h.Opts.MsgLightBgColor
		msgAbsentColor = h.Opts.MsgAbsentLightBgColor
	} else {
		msgColor = h.Opts.MsgDarkBgColor
		msgAbsentColor = h.Opts.MsgAbsentDarkBgColor
	}
	msgColor = color.New(color.FgHiWhite)
	msgAbsentColor = color.New(color.FgHiWhite)

	var msg string
	if h.Message == "" {
		msg = msgAbsentColor.Sprint("<no msg>")
	} else {
		msg = msgColor.Sprint(h.Message)
	}

	var level string
	switch h.Level {
	case "7":
		level = h.Opts.DebugLevelColor.Sprint("DEBU")
	case "5", "6":
		level = h.Opts.InfoLevelColor.Sprint("INFO")
	case "4":
		level = h.Opts.WarnLevelColor.Sprint("WARN")
	case "3":
		level = h.Opts.ErrorLevelColor.Sprint("ERRO")
	case "2", "1", "0":
		level = h.Opts.FatalLevelColor.Sprint("FATA")
	default:
		level = h.Opts.UnknownLevelColor.Sprint("UNKN")
	}

	var timeColor *color.Color
	if h.Opts.LightBg {
		timeColor = h.Opts.TimeLightBgColor
	} else {
		timeColor = h.Opts.TimeDarkBgColor
	}
	_, _ = fmt.Fprintf(h.out, "%s |%s| %s\t %s",
		timeColor.Sprint(h.Time.Format(h.Opts.TimeFormat)),
		level,
		msg,
		strings.Join(h.joinKVs(skipUnchanged, "="), "\t "),
	)

	_ = h.out.Flush()

	return h.buf.Bytes()
}

func (h *JournalJSONHandler) shouldShowKey(key string) bool {
	if len(h.Opts.Keep) != 0 {
		if _, keep := h.Opts.Keep[key]; keep {
			return true
		}
		if _, keep := h.Opts.Keep[strings.ToLower(key)]; keep {
			return true
		}
	}

	if len(h.Opts.Skip) != 0 {
		if _, skip := h.Opts.Skip[key]; skip {
			return false
		}
		if _, skip := h.Opts.Skip[strings.ToLower(key)]; skip {
			return false
		}
	}

	// definitely not a keep -- autoskip underscored
	if strings.HasPrefix(key, "_") {
		return false
	}

	return true
}

func (h *JournalJSONHandler) shouldShowUnchanged(key string) bool {
	if len(h.Opts.Keep) != 0 {
		if _, keep := h.Opts.Keep[key]; keep {
			return true
		}
	}
	if len(h.Opts.Keep) != 0 {
		if _, keep := h.Opts.Keep[strings.ToLower(key)]; keep {
			return true
		}
	}
	return false
}

func (h *JournalJSONHandler) joinKVs(skipUnchanged bool, sep string) []string {

	kv := make([]string, 0, len(h.Fields))
	for k, v := range h.Fields {
		if !h.shouldShowKey(k) {
			continue
		}

		if skipUnchanged {
			if lastV, ok := h.last[k]; ok && lastV == v && !h.shouldShowUnchanged(k) {
				continue
			}
		}
		kstr := h.Opts.KeyColor.Sprint(k)

		var vstr string
		if h.Opts.Truncates && len(v) > h.Opts.TruncateLength {
			vstr = v[:h.Opts.TruncateLength] + "..."
		} else {
			vstr = v
		}
		vstr = h.Opts.ValColor.Sprint(vstr)
		kv = append(kv, kstr+sep+vstr)
	}

	sort.Strings(kv)

	if h.Opts.SortLongest {
		sort.Stable(byLongest(kv))
	}

	return kv
}
