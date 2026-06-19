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

func TestParseEngineArgs(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "no parens at all",
			in:   "MergeTree ORDER BY id",
			want: "",
		},
		{
			name: "empty parens",
			in:   "MergeTree() ORDER BY id SETTINGS index_granularity = 8192",
			want: "",
		},
		{
			name: "ReplacingMergeTree with version column",
			in:   "ReplacingMergeTree(xo_received_at) ORDER BY id SETTINGS index_granularity = 8192",
			want: "xo_received_at",
		},
		{
			name: "SummingMergeTree with column list",
			in:   "SummingMergeTree(a, b, c) ORDER BY id",
			want: "a, b, c",
		},
		{
			name: "VersionedCollapsingMergeTree with two args",
			in:   "VersionedCollapsingMergeTree(sign, version) ORDER BY id",
			want: "sign, version",
		},
		{
			name: "nested parens in args",
			in:   "ReplacingMergeTree(toUInt64(modified_at)) ORDER BY id",
			want: "toUInt64(modified_at)",
		},
		{
			name: "quoted string with paren inside",
			in:   "Distributed('cluster', 'db', 'table_(2024)', rand())",
			want: "'cluster', 'db', 'table_(2024)', rand()",
		},
		{
			name: "Shared engine strips replication params",
			in:   "SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', xo_received_at) ORDER BY id",
			want: "xo_received_at",
		},
		{
			name: "Shared engine with only replication params yields empty",
			in:   "SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY id",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseEngineArgs(tt.in)
			if got != tt.want {
				t.Errorf("parseEngineArgs(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestNormalizeEngineArgs(t *testing.T) {
	tests := []struct {
		name       string
		engine     string
		engineFull string
		want       string
	}{
		{
			name:       "plain MergeTree drops deprecated positional key args",
			engine:     "MergeTree",
			engineFull: "MergeTree(ascap_program_code, cue_sequence_number) ORDER BY (ascap_program_code, cue_sequence_number) SETTINGS index_granularity = 8192",
			want:       "",
		},
		{
			name:       "modern plain MergeTree has no args",
			engine:     "MergeTree",
			engineFull: "MergeTree() ORDER BY id",
			want:       "",
		},
		{
			name:       "ReplacingMergeTree keeps version column",
			engine:     "ReplacingMergeTree",
			engineFull: "ReplacingMergeTree(xo_received_at) ORDER BY id",
			want:       "xo_received_at",
		},
		{
			name:       "CollapsingMergeTree keeps sign column",
			engine:     "CollapsingMergeTree",
			engineFull: "CollapsingMergeTree(sign) ORDER BY id",
			want:       "sign",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeEngineArgs(tt.engine, tt.engineFull)
			if got != tt.want {
				t.Errorf("normalizeEngineArgs(%q, %q) = %q, want %q", tt.engine, tt.engineFull, got, tt.want)
			}
		})
	}
}
