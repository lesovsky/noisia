package main

import "testing"

func TestResolveHotRows(t *testing.T) {
	testcases := []struct {
		name    string
		hotRows uint
		jobs    uint16
		want    int
	}{
		{name: "sentinel jobs=1 -> 1", hotRows: 0, jobs: 1, want: 1},
		{name: "sentinel jobs=9 -> 1", hotRows: 0, jobs: 9, want: 1},
		{name: "sentinel jobs=10 -> 1", hotRows: 0, jobs: 10, want: 1},
		{name: "sentinel jobs=20 -> 2", hotRows: 0, jobs: 20, want: 2},
		{name: "sentinel jobs=100 -> 10", hotRows: 0, jobs: 100, want: 10},
		{name: "explicit value passed through", hotRows: 5, jobs: 100, want: 5},
		{name: "explicit value with jobs=1", hotRows: 1, jobs: 1, want: 1},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveHotRows(tc.hotRows, tc.jobs)
			if got != tc.want {
				t.Errorf("resolveHotRows(%d, %d) = %d, want %d", tc.hotRows, tc.jobs, got, tc.want)
			}
		})
	}
}
