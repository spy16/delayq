package delayq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorker_sanitiseConfig(t *testing.T) {
	t.Run("NilQueue", func(t *testing.T) {
		w := Worker{}
		err := w.sanitiseConfig()
		assert.Error(t, err)
		assert.EqualError(t, err, "queue is not set, nothing to do")
	})

	t.Run("Success", func(t *testing.T) {
		q := &fakeQueue{}
		invoke := func(ctx context.Context, item Item) error {
			return nil
		}

		w := Worker{
			Queue:  q,
			Invoke: invoke,
		}
		err := w.sanitiseConfig()
		assert.NoError(t, err)
		assert.Equal(t, w.Workers, 1)
		assert.Equal(t, w.PollInterval, 500*time.Millisecond)
	})
}

type fakeQueue struct {
	DelayQ
}
