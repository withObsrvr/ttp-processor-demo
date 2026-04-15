package main

import "strconv"

// parseBalanceText converts either a raw stroops string ("12345") or a
// decimal string with 7dp ("12.3456789") into stroops.
func parseBalanceText(v *string) *int64 {
	if v == nil || *v == "" {
		return nil
	}
	s := *v
	if !containsDot(s) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil
		}
		return &n
	}

	wholeFrac := splitOnce(s, '.')
	whole, err := strconv.ParseInt(wholeFrac[0], 10, 64)
	if err != nil {
		return nil
	}
	fracStr := wholeFrac[1]
	for len(fracStr) < 7 {
		fracStr += "0"
	}
	if len(fracStr) > 7 {
		fracStr = fracStr[:7]
	}
	frac, err := strconv.ParseInt(fracStr, 10, 64)
	if err != nil {
		return nil
	}
	v64 := whole*10000000 + frac
	return &v64
}

func containsDot(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return true
		}
	}
	return false
}

func splitOnce(s string, sep byte) [2]string {
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			return [2]string{s[:i], s[i+1:]}
		}
	}
	return [2]string{s, ""}
}

func nullableInt32Pointer(v *int32) any {
	if v == nil {
		return nil
	}
	return int(*v)
}
