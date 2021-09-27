package queue

import (
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	q := MessageQueue{}
	q.Init()
}

func TestOrder(t *testing.T) {
	q := MessageQueue{}
	q.Init()
	q.Insert([]byte("MSG 1"))
	q.Insert([]byte("MSG 2"))
}

func TestInsert(t *testing.T) {
	q := MessageQueue{}
	q.Init()
	q.Insert([]byte("MSG 1"))
	q.Insert([]byte("MSG 2"))
	time.Sleep(100 * time.Millisecond)

	// There should be a message on the channel
	select {
	case msg := <-q.Receive:
		if string(msg) != "MSG 1" {
			t.Error("Message is not the same as inserted:", msg)
		}
	default:
		t.Error("Failed to send signal for receive")
	}

	select {
	case msg := <-q.Receive:
		if string(msg) != "MSG 2" {
			t.Error("Message 2 is not the same as inserted:", msg)
		}
	default:
		t.Error("Failed to send signal for receive")
	}

}

func TestTimeQueue(t *testing.T) {
	q := MessageQueue{}
	q.Init()

	message := []byte("RG8geW91IGhhdmUgdG8gZGVhbCB3aXRoIEJhc2U2NCBmb3JtYXQ/IFRoZW4gdGhpcyBzaXRlIGlzIHBlcmZlY3QgZm9yIHlvdSEgVXNlIG91ciBzdXBlciBoYW5keSBvbmxpbmUgdG9vbCB0byBlbmNvZGUgb3IgZGVjb2RlIHlvdXIgZGF0YS4=")
	numEntries := 100000

	// Time to insert
	start := time.Now()
	for i := 0; i < numEntries; i++ {
		q.Insert(message)
	}
	elapsed := time.Since(start)
	t.Log("Insert took:", elapsed)

	start = time.Now()
	for i := 0; i < numEntries; i++ {
		<-q.Receive
	}
	elapsed = time.Since(start)
	t.Log("Receive took:", elapsed)
	t.Log("Time:", elapsed/100000, "per op")
	t.Logf("Ops per second: %.2f", 1/((elapsed / 100000).Seconds()))
}
