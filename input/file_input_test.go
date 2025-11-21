package input

import (
	"encoding/base64"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestFileReader(t *testing.T) {
	// Prepare temporary file with two JSON lines
	tmpfile, err := ioutil.TempFile("", "filereader_test_*.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	data1 := []byte("hello world")
	data2 := []byte("goodbye")

	line1 := `{"remote":"127.0.0.1","version":"0.1.0","data":"` + base64.StdEncoding.EncodeToString(data1) + `"}` + "\n"
	line2 := `{"remote":"127.0.0.1","version":"0.1.0","data":"` + base64.StdEncoding.EncodeToString(data2) + `"}` + "\n"

	if _, err := tmpfile.WriteString(line1); err != nil {
		t.Fatal(err)
	}
	if _, err := tmpfile.WriteString(line2); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	fr := NewFileReader(tmpfile.Name(), true)
	if err := fr.Start(); err != nil {
		t.Fatalf("failed to start FileReader: %v", err)
	}

	// Collect packets
	got := make([][]byte, 0)
	timeout := time.After(2 * time.Second)
Loop:
	for {
		select {
		case pkt, ok := <-fr.Packets():
			if !ok {
				break Loop
			}
			got = append(got, pkt)
		case <-timeout:
			t.Fatal("timeout waiting for packets")
		}
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 packets, got %d", len(got))
	}

	if string(got[0]) != string(data1) {
		t.Fatalf("packet 1 mismatch: want %q got %q", string(data1), string(got[0]))
	}
	if string(got[1]) != string(data2) {
		t.Fatalf("packet 2 mismatch: want %q got %q", string(data2), string(got[1]))
	}

	// Stop reader
	fr.Stop()
}
