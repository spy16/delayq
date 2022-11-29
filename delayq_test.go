package delayq_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/spy16/delayq"
)

func TestItem_JSON(t *testing.T) {
	item := delayq.Item{
		At:    time.Unix(100, 0),
		Value: "hello",
	}

	jsonStr := item.JSON()
	assert.Equal(t, `{"at":100,"value":"hello"}`, jsonStr)
}

func TestItem_MarshalJSON(t *testing.T) {
	item := delayq.Item{
		At:    time.Unix(100, 0),
		Value: "hello",
	}

	b, err := item.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, `{"at":100,"value":"hello"}`, string(b))
}

func TestItem_UnmarshalJSON(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		const jsonStr = `{"at":100,"value":"hello"}`
		var item delayq.Item
		err := item.UnmarshalJSON([]byte(jsonStr))
		assert.NoError(t, err)
		assert.Equal(t, delayq.Item{
			At:    time.Unix(100, 0).UTC(),
			Value: "hello",
		}, item)
	})

	t.Run("Error", func(t *testing.T) {
		var item delayq.Item
		err := item.UnmarshalJSON([]byte("{"))
		assert.Error(t, err)
	})
}
