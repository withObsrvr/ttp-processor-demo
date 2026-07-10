package main

func ledgerRangeBounds(start, end int64) (int64, int64) {
	const rangeSize int64 = 10000
	if start < 0 {
		start = 0
	}
	if end < start {
		end = start
	}
	return (start / rangeSize) * rangeSize, (end / rangeSize) * rangeSize
}
