package record

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"testing"
)

func sample() *Record {
	return &Record{
		Offset:      42,
		Timestamp:   1_700_000_000_000,
		LeaderEpoch: 3,
		ProducerID:  7,
		Sequence:    11,
		Key:         []byte("user-123"),
		Value:       []byte(`{"amount":10}`),
		Headers: []Header{
			{Key: "event_type", Value: []byte("order.created")},
			{Key: "schema_version", Value: []byte("2")},
		},
	}
}

func TestRoundTrip(t *testing.T) {
	r := sample()
	b := Encode(r)
	if len(b) != r.EncodedSize() {
		t.Fatalf("size %d, want %d", len(b), r.EncodedSize())
	}
	got, n, err := Decode(b, DefaultMaxRecordSize)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(b) {
		t.Fatalf("consumed %d of %d", n, len(b))
	}
	if got.Offset != 42 || got.LeaderEpoch != 3 || got.ProducerID != 7 || got.Sequence != 11 {
		t.Fatalf("fixed fields mismatch: %+v", got)
	}
	if string(got.Key) != "user-123" || string(got.Value) != `{"amount":10}` {
		t.Fatalf("payload mismatch: %+v", got)
	}
	if v, _ := got.HeaderValue("schema_version"); string(v) != "2" {
		t.Fatalf("header mismatch")
	}
	m, err := ParseMeta(b, 0)
	if err != nil || m.Offset != 42 || m.Size != len(b) || m.Timestamp != r.Timestamp {
		t.Fatalf("meta %+v err %v", m, err)
	}
	if FrameOffset(b) != 42 || FrameLeaderEpoch(b) != 3 || FrameTimestamp(b) != r.Timestamp {
		t.Fatal("frame accessors")
	}
}

func TestNullKeyAndTombstone(t *testing.T) {
	r := &Record{Offset: 1, Sequence: -1, Tombstone: true, Key: []byte("k")}
	got, _, err := Decode(Encode(r), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Tombstone || got.Value != nil {
		t.Fatalf("tombstone lost: %+v", got)
	}
	r2 := &Record{Offset: 2, Value: []byte{}}
	got2, _, err := Decode(Encode(r2), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got2.Key != nil || got2.Value == nil || got2.Tombstone {
		t.Fatalf("null key / empty value confusion: %+v", got2)
	}
}

func TestCorruptionDetected(t *testing.T) {
	b := Encode(sample())
	for i := FrameHeaderSize; i < len(b); i++ {
		c := append([]byte(nil), b...)
		c[i] ^= 0x40
		if _, _, err := Decode(c, 0); !errors.Is(err, ErrChecksum) {
			t.Fatalf("flip at %d: err=%v, want checksum", i, err)
		}
	}
}

func TestTruncationDetected(t *testing.T) {
	b := Encode(sample())
	for n := 0; n < len(b); n++ {
		_, _, err := Decode(b[:n], 0)
		if err == nil {
			t.Fatalf("decoded truncated frame of %d bytes", n)
		}
	}
	br := bufio.NewReader(bytes.NewReader(b[:len(b)-3]))
	if _, err := ReadFrame(br, nil, 0); !errors.Is(err, ErrTruncated) {
		t.Fatalf("ReadFrame partial: %v", err)
	}
	br = bufio.NewReader(bytes.NewReader(nil))
	if _, err := ReadFrame(br, nil, 0); err != io.EOF {
		t.Fatalf("ReadFrame empty: %v", err)
	}
}

func TestMaxSizeEnforced(t *testing.T) {
	r := &Record{Value: make([]byte, 2000)}
	if err := r.Validate(1000); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("validate: %v", err)
	}
	if _, _, err := Decode(Encode(r), 1000); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("decode: %v", err)
	}
}

func TestSplitFrames(t *testing.T) {
	var buf []byte
	for i := 0; i < 5; i++ {
		buf = AppendEncode(buf, &Record{Offset: int64(i), Value: []byte("v")})
	}
	metas, err := SplitFrames(buf, 0)
	if err != nil || len(metas) != 5 || metas[4].Offset != 4 {
		t.Fatalf("metas=%v err=%v", metas, err)
	}
	recs, err := DecodeAll(buf, 0)
	if err != nil || len(recs) != 5 {
		t.Fatalf("decodeAll %d %v", len(recs), err)
	}
}

// FuzzDecode is a property test: Decode must never panic, and anything it
// accepts must re-encode to identical bytes.
func FuzzDecode(f *testing.F) {
	f.Add(Encode(sample()))
	f.Add(Encode(&Record{Tombstone: true}))
	f.Add([]byte{0, 0, 0, 1, 2})
	f.Fuzz(func(t *testing.T, b []byte) {
		r, n, err := Decode(b, DefaultMaxRecordSize)
		if err != nil {
			return
		}
		again := Encode(&r)
		if !bytes.Equal(again, b[:n]) {
			t.Fatalf("re-encode mismatch")
		}
	})
}

func BenchmarkEncode(b *testing.B) {
	r := sample()
	r.Value = make([]byte, 1024)
	buf := make([]byte, 0, 4096)
	b.SetBytes(int64(r.EncodedSize()))
	for i := 0; i < b.N; i++ {
		buf = AppendEncode(buf[:0], r)
	}
}

func BenchmarkDecode(b *testing.B) {
	r := sample()
	r.Value = make([]byte, 1024)
	frame := Encode(r)
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		if _, _, err := Decode(frame, 0); err != nil {
			b.Fatal(err)
		}
	}
}
