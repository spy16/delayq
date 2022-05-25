package redis

import (
	"testing"
	"time"
)

func TestOptions_setDefaults(t *testing.T) {
	opts := Options{}
	opts.setDefaults()

	expect := Options{
		PreFetchCount: 20,
		ReclaimTTL:    3 * time.Minute,
		WriteShards:   1,
		ReadShards:    1,
	}

	if opts != expect {
		t.Errorf("\nwant=%+v,\ngot=%+v", expect, opts)
	}
}
