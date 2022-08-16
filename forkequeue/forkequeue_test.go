package forkequeue

import (
	"testing"

	"github.com/simon4545/forkequeue/internal/util"
	"github.com/stretchr/testify/assert"
)

func TestPush(t *testing.T) {
	Init(t.TempDir())

	data := PushData{Data: util.UniqRands(10, 1000)}
	result := Push("test", data)
	assert.Equal(t, result, true)
	Close()
}
func TestPop(b *testing.T) {
	Init(b.TempDir())
	data := PushData{Data: 1}
	Push("test", data)
	data1, _ := Pop("test")
	assert.NotEmpty(b, data1)
	Close()
}

func TestPopWithAck(b *testing.T) {
	Init(b.TempDir())
	data := PushData{Data: 1}
	Push("test", data)
	var data1 *PopData
	data1, _ = Pop("test")
	ack := FinishAckData{ID: data1.ID}
	assert.Equal(b, true, Ack("test", ack))
	Close()
}
func TestPopZero(b *testing.T) {
	Init(b.TempDir())
	// var data1 *PopData
	var err error
	_, err = Pop("test")
	assert.NotEmpty(b, err)
	Close()
}
func TestStat(b *testing.T) {
	Init(b.TempDir())
	data := PushData{Data: 1}
	Push("test", data)
	data1, err := Stats("test")

	b.Logf("TestStat %+v", data1)
	assert.Empty(b, err)
}
