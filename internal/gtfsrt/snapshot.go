// Package gtfsrt reads GTFS-Realtime feeds.
//
// One feed is one protobuf payload over HTTP. The package does not normalise
// the payload, because GTFS-Realtime has a single schema and the generated
// bindings already give a stable shape. The GBFS exporter needs a normalising
// layer only because GBFS changed its JSON between versions.
package gtfsrt

import (
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
)

// Snapshot is one reading of one feed.
//
// The client returns a snapshot even when the read fails, so that the
// collector can always report the size and the duration of the attempt.
type Snapshot struct {
	// Message is the decoded feed. It is nil when the read or the decode
	// failed.
	Message *gtfs.FeedMessage
	// Bytes is the size of the payload that the server sent.
	Bytes int
	// Duration is the time that the fetch and the decode took together.
	Duration time.Duration
	// FetchedAt is the time that the exporter finished the read. Use it, and
	// not time.Now, to age the entities of this snapshot, so that every age in
	// one scrape shares one reference point.
	FetchedAt time.Time
}

// OK reports whether the client decoded the feed.
func (s *Snapshot) OK() bool { return s != nil && s.Message != nil }

// Header returns the feed header, or nil when there is none.
func (s *Snapshot) Header() *gtfs.FeedHeader {
	if !s.OK() {
		return nil
	}
	return s.Message.GetHeader()
}

// Entities returns the entities of the feed, or nil when there are none.
func (s *Snapshot) Entities() []*gtfs.FeedEntity {
	if !s.OK() {
		return nil
	}
	return s.Message.GetEntity()
}
