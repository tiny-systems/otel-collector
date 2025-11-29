package unsafeconv

import "unsafe"

// String converts byte slice to string.
func String(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
