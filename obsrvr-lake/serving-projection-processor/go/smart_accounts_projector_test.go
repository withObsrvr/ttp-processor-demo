package main

import "testing"

func TestShouldBlockSmartAccountServingShrink(t *testing.T) {
	t.Setenv("SERVING_SMART_ACCOUNTS_ALLOW_SHRINK", "")

	tests := []struct {
		name     string
		source   int64
		existing int64
		want     bool
	}{
		{name: "fresh bootstrap", source: 0, existing: 0, want: false},
		{name: "small fixture", source: 2, existing: 2, want: false},
		{name: "historical replay preserved", source: 8875, existing: 8875, want: false},
		{name: "modest drift allowed", source: 8500, existing: 8875, want: false},
		{name: "hot reset blocks destructive rebuild", source: 2, existing: 8875, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldBlockSmartAccountServingShrink(tc.source, tc.existing); got != tc.want {
				t.Fatalf("shouldBlockSmartAccountServingShrink(%d, %d) = %v, want %v", tc.source, tc.existing, got, tc.want)
			}
		})
	}
}

func TestShouldBlockSmartAccountServingShrinkOverride(t *testing.T) {
	t.Setenv("SERVING_SMART_ACCOUNTS_ALLOW_SHRINK", "true")
	if shouldBlockSmartAccountServingShrink(2, 8875) {
		t.Fatal("override should allow intentional serving shrink")
	}
}
