package delayq

import (
	"testing"
	"time"
)

func TestOptions_setDefaults(t *testing.T) {
	opts := Options{}
	opts.setDefaults()

	expect := Options{
		Workers:       10,
		PollInterval:  500 * time.Millisecond,
		PreFetchCount: 20,
		ReclaimTTL:    3 * time.Minute,
	}

	if opts != expect {
		t.Errorf("\nwant=%+v,\ngot=%+v", expect, opts)
	}
}
