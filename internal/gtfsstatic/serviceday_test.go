package gtfsstatic_test

import (
	"testing"
	"time"

	"github.com/raspbeguy/gtfs_exporter/internal/gtfsstatic"
)

// TestTheServiceDayOriginIsNoonMinusTwelveHours guards the trap that the
// proof-of-concept for this feature fell into.
//
// GTFS measures a stop time from noon minus twelve hours of the service day.
// That equals local midnight on every day except the two on which the clock
// changes, when it is out by exactly one hour. A delay computed from local
// midnight is then wrong by 3600 seconds, twice a year, in every network that
// observes a clock change.
func TestTheServiceDayOriginIsNoonMinusTwelveHours(t *testing.T) {
	testCases := []struct {
		zone   string
		day    string
		origin string // the wall clock of the origin, in the same zone
	}{
		{"Europe/Paris", "2026-06-15", "2026-06-15 00:00"}, // an ordinary day
		{"Europe/Paris", "2026-03-29", "2026-03-28 23:00"}, // the clock springs forward
		{"Europe/Paris", "2026-10-25", "2026-10-25 01:00"}, // the clock falls back
		{"America/New_York", "2026-03-08", "2026-03-07 23:00"},
		{"America/New_York", "2026-11-01", "2026-11-01 01:00"},
		{"UTC", "2026-03-29", "2026-03-29 00:00"}, // a zone with no change
	}
	for _, testCase := range testCases {
		t.Run(testCase.zone+" "+testCase.day, func(t *testing.T) {
			loc, err := time.LoadLocation(testCase.zone)
			if err != nil {
				t.Fatalf("cannot load %s: %v", testCase.zone, err)
			}
			day, err := time.ParseInLocation("2006-01-02", testCase.day, loc)
			if err != nil {
				t.Fatalf("cannot parse the day: %v", err)
			}
			got := gtfsstatic.ServiceDayOrigin(day, loc).In(loc).Format("2006-01-02 15:04")
			if got != testCase.origin {
				t.Errorf("got %s, want %s", got, testCase.origin)
			}
		})
	}
}

// TestTheTimezoneDatabaseIsEmbedded checks that the binary does not depend on
// the zoneinfo files of the host.
//
// The container image ships tzdata, but a released tarball can run on a bare
// Alpine host or in a scratch image, where the delay would fail there and
// nowhere else.
func TestTheTimezoneDatabaseIsEmbedded(t *testing.T) {
	for _, name := range []string{"Europe/Paris", "America/New_York", "Australia/Sydney"} {
		if _, err := time.LoadLocation(name); err != nil {
			t.Errorf("cannot load %s: %v", name, err)
		}
	}
}

const inServiceFeed = "feed_publisher_name,feed_version,feed_start_date,feed_end_date\n" +
	"Star,v1,20260818,20260830\n"

// TestInServiceReadsTheValidityWindow checks the window that stops a stale
// timetable from producing a wrong delay.
//
// A dataset expires quickly. Rennes publishes a twelve day window and MBTA a
// twenty five day one.
func TestInServiceReadsTheValidityWindow(t *testing.T) {
	body := scheduleFixture(t, map[string]string{
		"stop_times.txt": threeStops,
		"feed_info.txt":  inServiceFeed,
	})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	day := func(s string) time.Time {
		parsed, err := time.Parse("2006-01-02", s)
		if err != nil {
			t.Fatalf("cannot parse %s: %v", s, err)
		}
		return parsed
	}
	for _, testCase := range []struct {
		date string
		want bool
	}{
		{"2026-08-17", false}, // the day before it starts
		{"2026-08-18", true},  // the first day
		{"2026-08-25", true},
		{"2026-08-30", true},  // the last day, inclusive
		{"2026-08-31", false}, // expired
	} {
		if got := index.InService(day(testCase.date)); got != testCase.want {
			t.Errorf("%s: got %v, want %v", testCase.date, got, testCase.want)
		}
	}

	expires, ok := index.ExpiresAt()
	if !ok {
		t.Fatal("the feed states an end date")
	}
	if want := day("2026-08-31"); !expires.Equal(want) {
		t.Errorf("got expiry %s, want %s", expires, want)
	}
}

// TestAFeedWithNoWindowIsAlwaysInService checks that an optional field stays
// optional. Refusing to answer would punish a feed for omitting it.
func TestAFeedWithNoWindowIsAlwaysInService(t *testing.T) {
	body := scheduleFixture(t, map[string]string{"stop_times.txt": threeStops})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	if !index.InService(time.Now()) {
		t.Error("a feed with no window must count as in service")
	}
	if _, ok := index.ExpiresAt(); ok {
		t.Error("a feed with no dates must state no expiry")
	}
}

// TestTheCalendarSuppliesTheExpiryWhenFeedInfoDoesNot checks the fallback.
//
// feed_info.txt and its date fields are both optional in GTFS. Measured
// against three real feeds, the widest service window agreed with
// feed_end_date every time.
func TestTheCalendarSuppliesTheExpiryWhenFeedInfoDoesNot(t *testing.T) {
	body := scheduleFixture(t, map[string]string{
		"stop_times.txt": threeStops,
		"calendar.txt": "service_id,monday,start_date,end_date\n" +
			"s1,1,20260818,20260830\ns2,1,20260818,20260901\n",
	})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	expires, ok := index.ExpiresAt()
	if !ok {
		t.Fatal("the calendar must supply an expiry")
	}
	want, _ := time.Parse("2006-01-02", "2026-09-02")
	if !expires.Equal(want) {
		t.Errorf("got %s, want the widest window %s", expires, want)
	}
}

// TestCalendarDatesAloneSupplyTheExpiry checks the other half of the fallback.
// SNCF has no calendar.txt window and expresses its whole service through
// calendar_dates.txt.
func TestCalendarDatesAloneSupplyTheExpiry(t *testing.T) {
	body := scheduleFixture(t, map[string]string{
		"stop_times.txt":     threeStops,
		"calendar_dates.txt": "service_id,date,exception_type\ns1,20260901,1\ns1,20270131,1\n",
	})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	expires, ok := index.ExpiresAt()
	if !ok {
		t.Fatal("calendar_dates.txt must supply an expiry")
	}
	want, _ := time.Parse("2006-01-02", "2027-02-01")
	if !expires.Equal(want) {
		t.Errorf("got %s, want %s", expires, want)
	}
}

// TestTheTimezoneComesFromTheAgencyOfTheRoute checks that a feed with agencies
// in several zones resolves the right one. Ile-de-France holds 60 agencies.
func TestTheTimezoneComesFromTheAgencyOfTheRoute(t *testing.T) {
	body := scheduleFixture(t, map[string]string{
		"agency.txt": "agency_id,agency_name,agency_timezone\n" +
			"paris,Paris,Europe/Paris\nboston,Boston,America/New_York\n",
		"routes.txt": "route_id,agency_id,route_short_name,route_type\n" +
			"r1,paris,a,1\nr2,boston,b,1\n",
		"stop_times.txt": threeStops,
	})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	for _, testCase := range []struct{ route, zone string }{
		{"r1", "Europe/Paris"},
		{"r2", "America/New_York"},
	} {
		route, ok := index.Route(testCase.route)
		if !ok {
			t.Fatalf("%s must be in the index", testCase.route)
		}
		loc, found := index.Location(route)
		if !found || loc.String() != testCase.zone {
			t.Errorf("%s: got %s and %v, want %s", testCase.route, loc, found, testCase.zone)
		}
	}
}

// TestExpiryPullsTheRefreshForward checks that the configured interval is a
// floor and the data supplies the deadline.
//
// The life of a dataset varies by a factor of twenty: Rennes publishes a
// twelve day window and SNCF a one hundred and sixty two day one. A flat
// interval serves neither well.
func TestExpiryPullsTheRefreshForward(t *testing.T) {
	body := scheduleFixture(t, map[string]string{
		"stop_times.txt": threeStops,
		"feed_info.txt":  inServiceFeed, // valid 20260818 to 20260830
	})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})
	expires, ok := index.ExpiresAt()
	if !ok {
		t.Fatal("the fixture states an end date")
	}

	testCases := []struct {
		name string
		now  time.Time
		want time.Duration
	}{
		{
			name: "far from the deadline, the interval wins",
			now:  expires.Add(-30 * 24 * time.Hour),
			want: 24 * time.Hour,
		},
		{
			name: "near the deadline, the deadline wins",
			now:  expires.Add(-60 * time.Hour),
			want: 12 * time.Hour, // 60h to expiry, renew at 48h, so 12h to wait
		},
		{
			name: "inside the margin, the retry gap wins",
			now:  expires.Add(-12 * time.Hour),
			want: time.Hour,
		},
		{
			name: "already expired, keep retrying",
			now:  expires.Add(72 * time.Hour),
			want: time.Hour,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got := gtfsstatic.NextRefreshForTest(index, testCase.now,
				24*time.Hour, 48*time.Hour, time.Hour)
			if got != testCase.want {
				t.Errorf("got %v, want %v", got, testCase.want)
			}
		})
	}
}

// TestAFeedWithNoExpiryUsesThePlainInterval checks the fallback when the data
// supplies no deadline at all.
func TestAFeedWithNoExpiryUsesThePlainInterval(t *testing.T) {
	body := scheduleFixture(t, map[string]string{"stop_times.txt": threeStops})
	index := loadWithSchedule(t, body, gtfsstatic.Feed{Schedule: true})

	got := gtfsstatic.NextRefreshForTest(index, time.Now(), 24*time.Hour, 48*time.Hour, time.Hour)
	if got != 24*time.Hour {
		t.Errorf("got %v, want the plain interval", got)
	}
}
