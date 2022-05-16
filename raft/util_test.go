package raft

import (
	"reflect"
	"testing"
)

func Test_copySlice(t *testing.T) {
	type args struct {
		src  []LogEntry
		from int
		data []LogEntry
	}
	tests := []struct {
		name string
		args args
		want []LogEntry
	}{
		{
			"no wrap",
			args{
				[]LogEntry{{-1, 1}, {1, 1}, {2, 1}},
				3,
				[]LogEntry{{3, 1}},
			},
			[]LogEntry{{-1, 1}, {1, 1}, {2, 1}, {3, 1}},
		},
		{
			"partial wrap",
			args{
				[]LogEntry{{-1, 1}, {1, 1}, {2, 1}},
				2,
				[]LogEntry{{3, 1}, {4, 1}},
			},
			[]LogEntry{{-1, 1}, {1, 1}, {3, 1}, {4, 1}},
		},
		{
			"full wrap with distinction",
			args{
				[]LogEntry{{-1, 1}, {1, 1}, {2, 1}, {3, 1}},
				1,
				[]LogEntry{{5, 1}, {6, 1}},
			},
			[]LogEntry{{-1, 1}, {5, 1}, {6, 1}, {3, 1}},
		},
		{
			"full wrap without distinction",
			args{
				[]LogEntry{{-1, 1}, {1, 1}, {2, 1}, {3, 1}},
				1,
				[]LogEntry{{1, 1}, {2, 1}},
			},
			[]LogEntry{{-1, 1}, {1, 1}, {2, 1}, {3, 1}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := copySlice(tt.args.src, tt.args.from, tt.args.data); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("copySlice() = %v, want %v", got, tt.want)
			}
		})
	}
}
