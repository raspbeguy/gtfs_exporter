// Package gtfsstatic reads the parts of a static GTFS feed that a realtime
// feed cannot supply.
//
// A realtime feed often names a trip and no route. SNCF publishes 1140 trip
// updates with no route identifier on any of them. The index answers the
// question "which route runs this trip".
//
// The package reads four members of the zip: agency.txt, routes.txt,
// trips.txt, and feed_info.txt. It never reads stop_times.txt, which is 1.5 GB
// uncompressed for Ile-de-France, nor shapes.txt, which is 991 MB.
package gtfsstatic

import (
	"time"
)

// Route is one row of routes.txt.
//
// AgencyID is the identifier and not the name. A metric that carries a name
// changes its series every time an operator renames a route.
type Route struct {
	ID        string
	ShortName string
	// Type is the raw route_type. GTFS defines extended values, and
	// Ile-de-France publishes 0, 1, 3, 6, 7, 108, 109, 200, 702 and 715, so
	// never validate it against the basic range of 0 to 12.
	Type     string
	AgencyID string
}

// FeedInfo is the row of feed_info.txt. The file is 234 bytes and it names the
// dataset, so a reader can tell which version produced a mismatch.
type FeedInfo struct {
	Version   string
	StartDate string
	EndDate   string
}

// Index holds the routes of a static feed and the route of every trip.
//
// An index is immutable once built. The loader replaces a whole index, so a
// reader needs no lock.
type Index struct {
	routes []Route
	// byRoute and byTrip hold an offset into routes rather than a copy of the
	// identifier. Ile-de-France has 628893 trips and only 1973 routes, so a
	// map of string to string would waste tens of megabytes.
	byRoute  map[string]uint32
	byTrip   map[string]uint32
	agencies int
	// agencyTZ maps an agency to its timezone. A feed can hold agencies in
	// different zones, so the delay resolves the zone through the route.
	agencyTZ map[string]*time.Location
	feed     FeedInfo
	// calendarEnd is the widest service end of calendar.txt and
	// calendar_dates.txt, used when feed_info.txt states no end date.
	calendarEnd string
	// schedule is nil unless the module asked for it. The route index is
	// cheap, the timetable is not.
	schedule *Schedule
	loadedAt time.Time
	took     time.Duration
	bytes    int64
}

// RouteOfTrip returns the route that runs the trip.
func (i *Index) RouteOfTrip(tripID string) (Route, bool) {
	if i == nil {
		return Route{}, false
	}
	offset, ok := i.byTrip[tripID]
	if !ok {
		return Route{}, false
	}
	return i.routes[offset], true
}

// Route returns the route with the identifier.
func (i *Index) Route(routeID string) (Route, bool) {
	if i == nil {
		return Route{}, false
	}
	offset, ok := i.byRoute[routeID]
	if !ok {
		return Route{}, false
	}
	return i.routes[offset], true
}

// Schedule returns the timetable, or nil when the module did not ask for one.
func (i *Index) Schedule() *Schedule {
	if i == nil {
		return nil
	}
	return i.schedule
}

// FeedInfo returns the dataset metadata.
func (i *Index) FeedInfo() FeedInfo {
	if i == nil {
		return FeedInfo{}
	}
	return i.feed
}

// Stats returns the size of the index.
func (i *Index) Stats() (agencies, routes, trips int) {
	if i == nil {
		return 0, 0, 0
	}
	return i.agencies, len(i.routes), len(i.byTrip)
}

// LoadedAt returns the time that the index was built.
func (i *Index) LoadedAt() time.Time {
	if i == nil {
		return time.Time{}
	}
	return i.loadedAt
}

// Took returns the time that the load needed.
func (i *Index) Took() time.Duration {
	if i == nil {
		return 0
	}
	return i.took
}

// Bytes returns the number of bytes that the load read from the zip. Compare
// it with the size of the file to see what the range requests saved.
func (i *Index) Bytes() int64 {
	if i == nil {
		return 0
	}
	return i.bytes
}
