package clickhouse

import (
	"reflect"
	"testing"
)

func TestParseSettings(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want map[string]string
	}{
		{
			name: "no SETTINGS clause",
			in:   "MergeTree() ORDER BY id",
			want: nil,
		},
		{
			name: "single setting",
			in:   "MergeTree() ORDER BY id SETTINGS index_granularity = 8192",
			want: map[string]string{"index_granularity": "8192"},
		},
		{
			name: "multiple settings",
			in:   "MergeTree() ORDER BY id SETTINGS index_granularity = 8192, allow_nullable_key = 1",
			want: map[string]string{
				"index_granularity":  "8192",
				"allow_nullable_key": "1",
			},
		},
		{
			name: "quoted string value with comma inside",
			in:   "MergeTree() ORDER BY id SETTINGS storage_policy = 'tier_a,tier_b', index_granularity = 8192",
			want: map[string]string{
				"storage_policy":    "'tier_a,tier_b'",
				"index_granularity": "8192",
			},
		},
		{
			name: "values are kept verbatim (quotes preserved)",
			in:   "MergeTree() ORDER BY id SETTINGS storage_policy = 'default'",
			want: map[string]string{"storage_policy": "'default'"},
		},
		{
			name: "empty SETTINGS suffix returns nil",
			in:   "MergeTree() ORDER BY id SETTINGS ",
			want: nil,
		},
		{
			name: "engine_full with PARTITION BY then SETTINGS",
			in:   "MergeTree() PARTITION BY toYYYYMM(date) ORDER BY id SETTINGS index_granularity = 8192",
			want: map[string]string{"index_granularity": "8192"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseSettings(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseSettings(%q) = %#v, want %#v", tt.in, got, tt.want)
			}
		})
	}
}
