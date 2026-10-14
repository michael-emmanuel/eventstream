// Package record defines the versioned binary format of a single event as it
// is stored on disk and shipped between replicas.
//
// The storage format is deliberately independent of the HTTP/JSON API: the
// API layer converts JSON payloads into Records, and the storage and
// replication layers only ever see the binary frames defined here. That lets
// the wire API evolve (or be replaced by a binary protocol) without rewriting
// stored data, and lets followers copy frames byte-for-byte from the leader.
//
// Frame layout (all integers big-endian):
//
//	+---------+---------+------------------------------------------------+
//	| length  | crc32c  | body (length - 4 bytes)                        |
//	| uint32  | uint32  |                                                |
//	+---------+---------+------------------------------------------------+
//
//	length   number of bytes that follow the length field (crc + body)
//	crc32c   Castagnoli CRC over the body
//
// Body (format version 1):
//
//	version      uint8   format version (1)
//	attributes   uint8   bit0 = tombstone (null value)
//	leaderEpoch  int32   epoch of the leader that first appended the record
//	offset       int64   partition-local offset
//	timestamp    int64   broker log-append time, unix milliseconds
//	producerID   int64   0 if the producer is not idempotent
//	sequence     int64   per-partition producer sequence, -1 if none
//	keyLen       int32   -1 for a null key
//	key          []byte
//	valueLen     int32   -1 for a tombstone
//	value        []byte
//	headerCount  uint16
//	  keyLen     uint16
//	  key        []byte  (UTF-8)
//	  valueLen   int32
//	  value      []byte
//
// Topic and partition are not stored per record: they are implied by the
// directory the segment lives in. This saves bytes on every event and is the
// reason the conceptual Event has more fields than the stored frame.
package record

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	// Version1 is the only on-disk format version currently written.
	Version1 byte = 1
	// CurrentVersion is the format version written by Encode.
	CurrentVersion = Version1

	// FrameHeaderSize is the length prefix plus the CRC.
	FrameHeaderSize = 8
	// fixedBodySize is the size of the body without key, value and headers.
	fixedBodySize = 1 + 1 + 4 + 8 + 8 + 8 + 8 + 4 + 4 + 2
	// Overhead is the number of bytes a record costs beyond its key, value
	// and headers.
	Overhead = FrameHeaderSize + fixedBodySize

	// DefaultMaxRecordSize bounds a single encoded frame. Readers refuse
	// length prefixes above the configured maximum, which turns a corrupted
	// length field into a detectable error instead of a giant allocation.
	DefaultMaxRecordSize = 1 << 20

	// MaxHeaders bounds the number of headers on one record.
	MaxHeaders = 64

	attrTombstone = 1 << 0

	offsetPos      = FrameHeaderSize + 1 + 1 + 4
	leaderEpochPos = FrameHeaderSize + 1 + 1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Errors returned by decoding. They are distinct so recovery code can tell a
// torn write (truncated) from bit rot (checksum) from a format bug (malformed).
var (
	ErrTruncated          = errors.New("record: truncated frame")
	ErrChecksum           = errors.New("record: checksum mismatch")
	ErrMalformed          = errors.New("record: malformed frame")
	ErrUnsupportedVersion = errors.New("record: unsupported format version")
	ErrTooLarge           = errors.New("record: frame exceeds maximum size")
)

// Header is an application-level key/value attached to a record. Headers
// carry things like event_type, schema_version and trace context.
type Header struct {
	Key   string
	Value []byte
}

// Record is one event in a partition.
type Record struct {
	Offset      int64
	Timestamp   int64 // unix milliseconds, assigned by the leader
	LeaderEpoch int32
	ProducerID  int64
	Sequence    int64
	Key         []byte // nil means "no key"
	Value       []byte
	Tombstone   bool // true means the value is null (a delete marker for compacted topics)
	Headers     []Header
}

// Meta is the fixed-size portion of a record, available without allocating
// the key/value. Recovery, replication and index building only need Meta.
type Meta struct {
	Offset      int64
	Timestamp   int64
	LeaderEpoch int32
	ProducerID  int64
	Sequence    int64
	Tombstone   bool
	Size        int // full frame size including header
}

// HeaderValue returns the first header with the given key.
func (r *Record) HeaderValue(key string) ([]byte, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return h.Value, true
		}
	}
	return nil, false
}

// EncodedSize returns the size of the frame Encode would produce.
func (r *Record) EncodedSize() int {
	n := Overhead + len(r.Key)
	if !r.Tombstone {
		n += len(r.Value)
	}
	for _, h := range r.Headers {
		n += 2 + len(h.Key) + 4 + len(h.Value)
	}
	return n
}

// Validate checks limits that the encoder relies on.
func (r *Record) Validate(maxSize int) error {
	if len(r.Headers) > MaxHeaders {
		return fmt.Errorf("%w: %d headers (max %d)", ErrMalformed, len(r.Headers), MaxHeaders)
	}
	for _, h := range r.Headers {
		if len(h.Key) > 0xFFFF {
			return fmt.Errorf("%w: header key too long", ErrMalformed)
		}
	}
	if maxSize > 0 && r.EncodedSize() > maxSize {
		return fmt.Errorf("%w: %d bytes (max %d)", ErrTooLarge, r.EncodedSize(), maxSize)
	}
	return nil
}

// AppendEncode appends the frame for r to dst and returns the extended slice.
// Callers must call Validate first; AppendEncode does not re-check limits.
func AppendEncode(dst []byte, r *Record) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, FrameHeaderSize)...)
	bodyStart := len(dst)

	var attrs byte
	if r.Tombstone {
		attrs |= attrTombstone
	}
	dst = append(dst, CurrentVersion, attrs)
	dst = binary.BigEndian.AppendUint32(dst, uint32(r.LeaderEpoch))
	dst = binary.BigEndian.AppendUint64(dst, uint64(r.Offset))
	dst = binary.BigEndian.AppendUint64(dst, uint64(r.Timestamp))
	dst = binary.BigEndian.AppendUint64(dst, uint64(r.ProducerID))
	dst = binary.BigEndian.AppendUint64(dst, uint64(r.Sequence))
	if r.Key == nil {
		dst = binary.BigEndian.AppendUint32(dst, uint32(0xFFFFFFFF))
	} else {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Key)))
		dst = append(dst, r.Key...)
	}
	if r.Tombstone {
		dst = binary.BigEndian.AppendUint32(dst, uint32(0xFFFFFFFF))
	} else {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Value)))
		dst = append(dst, r.Value...)
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(r.Headers)))
	for _, h := range r.Headers {
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(h.Key)))
		dst = append(dst, h.Key...)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(h.Value)))
		dst = append(dst, h.Value...)
	}

	body := dst[bodyStart:]
	binary.BigEndian.PutUint32(dst[start:], uint32(len(body)+4))
	binary.BigEndian.PutUint32(dst[start+4:], crc32.Checksum(body, castagnoli))
	return dst
}

// Encode returns the frame for r.
func Encode(r *Record) []byte {
	return AppendEncode(make([]byte, 0, r.EncodedSize()), r)
}

// frameLen validates the length prefix of b and returns the full frame size.
func frameLen(b []byte, maxSize int) (int, error) {
	if len(b) < FrameHeaderSize {
		return 0, ErrTruncated
	}
	l := int(binary.BigEndian.Uint32(b))
	total := l + 4
	if l < 4+fixedBodySize {
		return 0, fmt.Errorf("%w: length %d below minimum", ErrMalformed, l)
	}
	if maxSize > 0 && total > maxSize {
		return 0, fmt.Errorf("%w: length %d", ErrTooLarge, total)
	}
	if len(b) < total {
		return 0, ErrTruncated
	}
	return total, nil
}

func verify(frame []byte, maxSize int) (int, error) {
	total, err := frameLen(frame, maxSize)
	if err != nil {
		return 0, err
	}
	body := frame[FrameHeaderSize:total]
	if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(frame[4:]) {
		return 0, ErrChecksum
	}
	if body[0] != Version1 {
		return 0, fmt.Errorf("%w: %d", ErrUnsupportedVersion, body[0])
	}
	return total, nil
}

// ParseMeta verifies the checksum of the frame at the start of b and returns
// its fixed fields. It does not allocate.
func ParseMeta(b []byte, maxSize int) (Meta, error) {
	total, err := verify(b, maxSize)
	if err != nil {
		return Meta{}, err
	}
	body := b[FrameHeaderSize:total]
	return Meta{
		Tombstone:   body[1]&attrTombstone != 0,
		LeaderEpoch: int32(binary.BigEndian.Uint32(body[2:])),
		Offset:      int64(binary.BigEndian.Uint64(body[6:])),
		Timestamp:   int64(binary.BigEndian.Uint64(body[14:])),
		ProducerID:  int64(binary.BigEndian.Uint64(body[22:])),
		Sequence:    int64(binary.BigEndian.Uint64(body[30:])),
		Size:        total,
	}, nil
}

// FrameOffset returns the offset stored in a frame without verifying it.
// Callers must only use it on frames that were verified when written or
// recovered.
func FrameOffset(frame []byte) int64 {
	return int64(binary.BigEndian.Uint64(frame[offsetPos:]))
}

// FrameTimestamp returns the timestamp stored in a frame without verifying it.
func FrameTimestamp(frame []byte) int64 {
	return int64(binary.BigEndian.Uint64(frame[offsetPos+8:]))
}

// FrameLeaderEpoch returns the leader epoch stored in a frame.
func FrameLeaderEpoch(frame []byte) int32 {
	return int32(binary.BigEndian.Uint32(frame[leaderEpochPos:]))
}

// Decode verifies and decodes the frame at the start of b. Key, Value and
// header values alias b; use Clone if b will be reused.
func Decode(b []byte, maxSize int) (Record, int, error) {
	total, err := verify(b, maxSize)
	if err != nil {
		return Record{}, 0, err
	}
	body := b[FrameHeaderSize:total]
	r := Record{
		Tombstone:   body[1]&attrTombstone != 0,
		LeaderEpoch: int32(binary.BigEndian.Uint32(body[2:])),
		Offset:      int64(binary.BigEndian.Uint64(body[6:])),
		Timestamp:   int64(binary.BigEndian.Uint64(body[14:])),
		ProducerID:  int64(binary.BigEndian.Uint64(body[22:])),
		Sequence:    int64(binary.BigEndian.Uint64(body[30:])),
	}
	p := 38
	readBytes := func(n int) ([]byte, bool) {
		if n < 0 || p+n > len(body) {
			return nil, false
		}
		v := body[p : p+n]
		p += n
		return v, true
	}
	readLen32 := func() (int, bool) {
		if p+4 > len(body) {
			return 0, false
		}
		v := int32(binary.BigEndian.Uint32(body[p:]))
		p += 4
		return int(v), true
	}
	kl, ok := readLen32()
	if !ok {
		return Record{}, 0, ErrMalformed
	}
	if kl >= 0 {
		if r.Key, ok = readBytes(kl); !ok {
			return Record{}, 0, fmt.Errorf("%w: key length", ErrMalformed)
		}
	} else if kl != -1 {
		return Record{}, 0, fmt.Errorf("%w: key length %d", ErrMalformed, kl)
	}
	vl, ok := readLen32()
	if !ok {
		return Record{}, 0, ErrMalformed
	}
	if vl >= 0 {
		if r.Tombstone {
			return Record{}, 0, fmt.Errorf("%w: tombstone with value", ErrMalformed)
		}
		if r.Value, ok = readBytes(vl); !ok {
			return Record{}, 0, fmt.Errorf("%w: value length", ErrMalformed)
		}
		if r.Value == nil {
			r.Value = []byte{}
		}
	} else if vl != -1 || !r.Tombstone {
		return Record{}, 0, fmt.Errorf("%w: value length %d", ErrMalformed, vl)
	}
	if p+2 > len(body) {
		return Record{}, 0, ErrMalformed
	}
	hc := int(binary.BigEndian.Uint16(body[p:]))
	p += 2
	if hc > MaxHeaders {
		return Record{}, 0, fmt.Errorf("%w: %d headers", ErrMalformed, hc)
	}
	if hc > 0 {
		r.Headers = make([]Header, 0, hc)
	}
	for i := 0; i < hc; i++ {
		if p+2 > len(body) {
			return Record{}, 0, ErrMalformed
		}
		hk := int(binary.BigEndian.Uint16(body[p:]))
		p += 2
		k, ok := readBytes(hk)
		if !ok {
			return Record{}, 0, ErrMalformed
		}
		hv, ok := readLen32()
		if !ok || hv < 0 {
			return Record{}, 0, ErrMalformed
		}
		v, ok := readBytes(hv)
		if !ok {
			return Record{}, 0, ErrMalformed
		}
		r.Headers = append(r.Headers, Header{Key: string(k), Value: v})
	}
	if p != len(body) {
		return Record{}, 0, fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(body)-p)
	}
	return r, total, nil
}

// DecodeAll decodes a buffer of concatenated frames.
func DecodeAll(b []byte, maxSize int) ([]Record, error) {
	var out []Record
	for len(b) > 0 {
		r, n, err := Decode(b, maxSize)
		if err != nil {
			return out, err
		}
		out = append(out, r)
		b = b[n:]
	}
	return out, nil
}

// Clone returns a deep copy of r that does not alias any decode buffer.
func (r Record) Clone() Record {
	c := r
	if r.Key != nil {
		c.Key = append([]byte(nil), r.Key...)
	}
	if r.Value != nil {
		c.Value = append([]byte{}, r.Value...)
	}
	if r.Headers != nil {
		c.Headers = make([]Header, len(r.Headers))
		for i, h := range r.Headers {
			c.Headers[i] = Header{Key: h.Key, Value: append([]byte(nil), h.Value...)}
		}
	}
	return c
}

// ReadFrame reads one frame from br into scratch (growing it if needed) and
// returns the frame bytes. It returns io.EOF only at a clean frame boundary;
// a partial frame yields ErrTruncated. It does not verify the checksum.
func ReadFrame(br *bufio.Reader, scratch []byte, maxSize int) ([]byte, error) {
	var hdr [4]byte
	n, err := io.ReadFull(br, hdr[:])
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil || n < 4 {
		return nil, ErrTruncated
	}
	l := int(binary.BigEndian.Uint32(hdr[:]))
	total := l + 4
	if l < 4+fixedBodySize {
		return nil, fmt.Errorf("%w: length %d below minimum", ErrMalformed, l)
	}
	if maxSize > 0 && total > maxSize {
		return nil, fmt.Errorf("%w: length %d", ErrTooLarge, total)
	}
	if cap(scratch) < total {
		scratch = make([]byte, total)
	}
	scratch = scratch[:total]
	copy(scratch, hdr[:])
	if _, err := io.ReadFull(br, scratch[4:]); err != nil {
		return nil, ErrTruncated
	}
	return scratch, nil
}

// SplitFrames verifies every frame in b and returns their metadata. It is
// used by followers to validate a replication payload before appending it.
func SplitFrames(b []byte, maxSize int) ([]Meta, error) {
	var metas []Meta
	for len(b) > 0 {
		m, err := ParseMeta(b, maxSize)
		if err != nil {
			return nil, err
		}
		metas = append(metas, m)
		b = b[m.Size:]
	}
	return metas, nil
}
