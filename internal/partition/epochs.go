package partition

import "sort"

// epochEntry records that leader epoch Epoch began writing at StartOffset.
type epochEntry struct {
	Epoch       int32
	StartOffset int64
}

// epochCache is the leader-epoch history of a replica's log. It answers the
// question a follower asks after a leader change: "up to which offset does
// your log agree with mine for epoch E?" That lets the follower truncate
// exactly the records the old leader wrote but never committed, instead of
// guessing with the high-water mark (which loses or keeps the wrong records
// in some double-failure sequences).
//
// It is rebuilt at startup from the leaderEpoch field stored in every record,
// so it needs no separate file.
type epochCache struct {
	entries []epochEntry
}

// assign records that epoch starts at offset if it is newer than the latest.
func (c *epochCache) assign(epoch int32, offset int64) {
	if n := len(c.entries); n > 0 {
		last := c.entries[n-1]
		if epoch <= last.Epoch {
			return
		}
		if offset < last.StartOffset {
			offset = last.StartOffset
		}
	}
	c.entries = append(c.entries, epochEntry{epoch, offset})
}

// latest returns the newest epoch, or -1.
func (c *epochCache) latest() int32 {
	if len(c.entries) == 0 {
		return -1
	}
	return c.entries[len(c.entries)-1].Epoch
}

// epochAt returns the epoch of the record just below offset (i.e. the epoch
// of the last record when offset is the log end offset), or -1 if unknown.
func (c *epochCache) epochAt(offset int64) int32 {
	e := int32(-1)
	for _, en := range c.entries {
		if en.StartOffset < offset {
			e = en.Epoch
		}
	}
	return e
}

// endOffsetFor returns the largest epoch <= epoch known to this log and the
// offset where that epoch ends (the start of the next epoch, or leo for the
// current one).
func (c *epochCache) endOffsetFor(epoch int32, leo int64) (int32, int64) {
	if len(c.entries) == 0 {
		return -1, leo
	}
	i := sort.Search(len(c.entries), func(i int) bool { return c.entries[i].Epoch > epoch }) - 1
	if i < 0 {
		// The requested epoch predates our entire history; we can only vouch
		// for data from our first epoch onwards.
		return epoch, c.entries[0].StartOffset
	}
	if i == len(c.entries)-1 {
		return c.entries[i].Epoch, leo
	}
	return c.entries[i].Epoch, c.entries[i+1].StartOffset
}

// truncateFrom drops history at or after offset (after a log truncation).
func (c *epochCache) truncateFrom(offset int64) {
	n := 0
	for _, e := range c.entries {
		if e.StartOffset < offset {
			c.entries[n] = e
			n++
		}
	}
	c.entries = c.entries[:n]
}

func (c *epochCache) reset() { c.entries = nil }
