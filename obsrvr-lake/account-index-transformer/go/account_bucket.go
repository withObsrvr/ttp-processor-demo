package main

import "hash/crc32"

const defaultAccountIndexBuckets = 256

func AccountBucket(accountID string, bucketCount int) int64 {
	if bucketCount <= 0 {
		bucketCount = defaultAccountIndexBuckets
	}
	return int64(crc32.ChecksumIEEE([]byte(accountID)) % uint32(bucketCount))
}
