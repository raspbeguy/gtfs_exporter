package gtfsstatic

import (
	"time"

	// The timezone database is embedded, so the binary does not depend on the
	// zoneinfo files of the host. The container image is safe without this,
	// because gcr.io/distroless/static ships tzdata, but the released tarballs
	// run anywhere: a bare Alpine host or a scratch image has no zoneinfo, and
	// the delay would then fail there and nowhere else. The cost is about
	// 450 KB.
	_ "time/tzdata"
)

// ServiceDayOrigin returns the instant that a GTFS stop time counts from.
//
// GTFS measures a stop time from noon minus twelve hours of the service day.
// That equals local midnight on every day except the two on which the clock
// changes, when it is out by exactly one hour:
//
//	2026-03-29 Europe/Paris     midnight 00:00 CET   origin 2026-03-28 23:00
//	2026-10-25 Europe/Paris     midnight 00:00 CEST  origin 2026-10-25 01:00
//
// Add works on absolute time, so subtracting twelve hours from noon gives the
// right answer. Building local midnight directly does not.
func ServiceDayOrigin(day time.Time, loc *time.Location) time.Time {
	year, month, date := day.Date()
	noon := time.Date(year, month, date, 12, 0, 0, 0, loc)
	return noon.Add(-12 * time.Hour)
}

// Location returns the timezone of the agency that owns the route.
//
// A feed can hold agencies in different zones. Ile-de-France holds 60
// agencies, so taking the first row would be wrong there. UTC is the fallback,
// and it is always a poor one, so the caller must treat a missing zone as a
// reason to publish no delay rather than a reason to guess.
func (i *Index) Location(route Route) (*time.Location, bool) {
	if i == nil {
		return time.UTC, false
	}
	if loc, ok := i.agencyTZ[route.AgencyID]; ok {
		return loc, true
	}
	// A feed with exactly one agency may leave agency_id blank everywhere.
	if len(i.agencyTZ) == 1 {
		for _, loc := range i.agencyTZ {
			return loc, true
		}
	}
	return time.UTC, false
}

// InService reports whether the day falls inside the validity window of the
// dataset.
//
// A dataset expires quickly. Rennes publishes a twelve day window and MBTA a
// twenty five day one, so a feed whose refresh has been failing for a
// fortnight is serving a timetable that no longer describes the service. A
// delay computed against it is wrong rather than missing, so the caller must
// stop timing when this returns false.
//
// A feed that states no window is treated as in service. Refusing to answer
// would punish a feed for omitting an optional field.
func (i *Index) InService(day time.Time) bool {
	if i == nil {
		return false
	}
	date := day.Format("20060102")
	if start := i.feed.StartDate; start != "" && date < start {
		return false
	}
	if end := i.feed.EndDate; end != "" && date > end {
		return false
	}
	return true
}

// ExpiresAt returns the last instant that the dataset describes, and whether
// the feed stated one.
//
// It reads feed_end_date when feed_info.txt gives one. Both the file and the
// field are optional in GTFS, so it falls back to the widest service window of
// calendar.txt and calendar_dates.txt. Measured against three feeds, the
// fallback agreed with feed_end_date every time, and SNCF needs it because it
// expresses its whole service through calendar_dates.txt.
func (i *Index) ExpiresAt() (time.Time, bool) {
	if i == nil {
		return time.Time{}, false
	}
	end := i.feed.EndDate
	if end == "" {
		end = i.calendarEnd
	}
	if end == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse("20060102", end)
	if err != nil {
		return time.Time{}, false
	}
	// The window covers the whole of its last day.
	return parsed.AddDate(0, 0, 1), true
}

// StartsAt returns the first day that the dataset describes.
func (i *Index) StartsAt() (time.Time, bool) {
	if i == nil || i.feed.StartDate == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse("20060102", i.feed.StartDate)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}
