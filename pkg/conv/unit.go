package conv

import (
	"strings"
)

const (
	UnitInvalid string = "_"

	UnitNone        string = ""
	UnitPercents    string = "percents"
	UnitUtilization string = "utilization"

	UnitNanoseconds  string = "nanoseconds"
	UnitMicroseconds string = "microseconds"
	UnitMilliseconds string = "milliseconds"
	UnitSeconds      string = "seconds"
	UnitDuration     string = "duration"

	UnitBytes     string = "bytes"
	UnitKilobytes string = "kilobytes"
	UnitMegabytes string = "megabytes"
	UnitGigabytes string = "gigabytes"
	UnitTerabytes string = "terabytes"

	UnitTime     string = "time"
	UnitUnixTime string = "unix-time"
)

func NormUnit(s string) string {
	switch s {
	case "", "1", "0", "none", "None":
		return UnitNone
	}

	if norm := fromString(s); norm != "" {
		return norm
	}

	s = strings.ToLower(s)

	if norm := fromString(s); norm != "" {
		return norm
	}

	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		return s
	}
	return "{" + s + "}"
}

func fromString(s string) string {
	switch s {
	case "percents", "%", "Percent", "percent":
		return UnitPercents
	case "utilization":
		return UnitUtilization

	case "nanoseconds", "ns", "nanosecond":
		return UnitNanoseconds
	case "microseconds", "us", "microsecond":
		return UnitMicroseconds
	case "milliseconds", "ms", "millisecond":
		return UnitMilliseconds
	case "seconds", "s", "sec", "second":
		return UnitSeconds

	case "bytes", "Bytes", "by", "byte", "Byte":
		return UnitBytes
	case "kilobytes", "kb", "kib", "kbyte":
		return UnitKilobytes
	case "megabytes", "mb", "mib", "mbyte":
		return UnitMegabytes
	case "gigabytes", "gb", "gib", "gbyte":
		return UnitGigabytes
	case "terabytes", "tb", "tib", "tbyte":
		return UnitTerabytes

	case "count", "Count":
		return "{count}"

	default:
		return ""
	}
}
