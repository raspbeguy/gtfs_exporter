package gtfsstatic

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// maxMemberBytes limits one member of the zip. trips.txt of Ile-de-France is
// 130 MB uncompressed, so the limit sits well above it.
const maxMemberBytes = 512 << 20

// build reads the members that the index needs and returns the index.
//
// The members are opened one at a time. shapes.txt is never opened, and
// neither is stop_times.txt unless the caller asked for a schedule, so their
// bytes are never fetched and never inflated.
func build(reader *zip.Reader, wantSchedule bool, maxRows int) (*Index, error) {
	agencies, singleAgency, zones, err := readAgencies(reader)
	if err != nil {
		return nil, err
	}
	index := &Index{
		byRoute:  map[string]uint32{},
		byTrip:   map[string]uint32{},
		agencies: agencies,
		agencyTZ: zones,
		loadedAt: time.Now(),
	}
	if err := readRoutes(reader, index, singleAgency); err != nil {
		return nil, err
	}
	if err := readTrips(reader, index); err != nil {
		return nil, err
	}
	index.feed = readFeedInfo(reader)
	index.calendarEnd = readCalendarEnd(reader)
	if !wantSchedule {
		return index, nil
	}
	// A schedule that does not fit must not take the route index down with
	// it. The routes are small and useful on their own, so log the refusal
	// and carry on without a timetable.
	schedule, err := readSchedule(reader, maxRows)
	if err != nil {
		return index, &ScheduleError{Err: err}
	}
	index.schedule = schedule
	return index, nil
}

// ScheduleError reports that the timetable could not be built, while the rest
// of the index is usable. The loader installs the index and warns.
type ScheduleError struct{ Err error }

func (e *ScheduleError) Error() string { return e.Err.Error() }
func (e *ScheduleError) Unwrap() error { return e.Err }

// rows opens one member and calls the function for every data row, with a map
// from column name to value.
//
// A missing optional member is not an error. feed_info.txt is optional in
// GTFS, and a feed without it still gives every other answer.
func rows(reader *zip.Reader, name string, required bool, each func(map[string]string) error) error {
	file, err := reader.Open(name)
	if err != nil {
		if required {
			return fmt.Errorf("gtfsstatic: cannot open %s: %w", name, err)
		}
		return nil
	}
	defer func() { _ = file.Close() }()

	records := csv.NewReader(&bomStripper{reader: io.LimitReader(file, maxMemberBytes)})
	records.ReuseRecord = true
	// GTFS files in the wild carry rows with a stray extra comma. A strict
	// field count would reject a whole dataset over one bad row.
	records.FieldsPerRecord = -1

	head, err := records.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("gtfsstatic: cannot read the header of %s: %w", name, err)
	}
	columns := make(map[string]int, len(head))
	for position, column := range head {
		columns[strings.TrimSpace(column)] = position
	}

	row := make(map[string]string, len(columns))
	for {
		record, err := records.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("gtfsstatic: cannot read %s: %w", name, err)
		}
		clear(row)
		for column, position := range columns {
			if position < len(record) {
				row[column] = record[position]
			}
		}
		if err := each(row); err != nil {
			return err
		}
	}
}

// readAgencies counts the agencies and returns the only identifier when there
// is exactly one.
//
// GTFS makes agency_id optional in routes.txt when the feed holds one agency.
// Rennes is such a feed. Without this fallback every route carries an empty
// agency label.
func readAgencies(reader *zip.Reader) (count int, single string, zones map[string]*time.Location, err error) {
	zones = map[string]*time.Location{}
	err = rows(reader, "agency.txt", true, func(row map[string]string) error {
		count++
		if count == 1 {
			single = row["agency_id"]
		}
		// A zone that Go cannot load is left out, and the delay then refuses
		// to guess rather than reporting a number that is hours wrong.
		if name := strings.TrimSpace(row["agency_timezone"]); name != "" {
			if loc, loadErr := time.LoadLocation(name); loadErr == nil {
				zones[row["agency_id"]] = loc
			}
		}
		return nil
	})
	if count != 1 {
		single = ""
	}
	return count, single, zones, err
}

// readCalendarEnd returns the widest service end of calendar.txt and
// calendar_dates.txt, as a GTFS date string.
//
// It is the fallback for a feed that states no feed_end_date. SNCF needs it:
// it has no calendar.txt window at all and expresses its whole service through
// calendar_dates.txt.
func readCalendarEnd(reader *zip.Reader) string {
	widest := ""
	keep := func(value string) {
		value = strings.TrimSpace(value)
		if len(value) == 8 && value > widest {
			widest = value
		}
	}
	_ = rows(reader, "calendar.txt", false, func(row map[string]string) error {
		keep(row["end_date"])
		return nil
	})
	_ = rows(reader, "calendar_dates.txt", false, func(row map[string]string) error {
		keep(row["date"])
		return nil
	})
	return widest
}

func readRoutes(reader *zip.Reader, index *Index, singleAgency string) error {
	return rows(reader, "routes.txt", true, func(row map[string]string) error {
		id := row["route_id"]
		if id == "" {
			return nil
		}
		agency := row["agency_id"]
		if agency == "" {
			agency = singleAgency
		}
		index.routes = append(index.routes, Route{
			ID:        id,
			ShortName: row["route_short_name"],
			Type:      row["route_type"],
			AgencyID:  agency,
		})
		index.byRoute[id] = uint32(len(index.routes) - 1)
		return nil
	})
}

func readTrips(reader *zip.Reader, index *Index) error {
	return rows(reader, "trips.txt", true, func(row map[string]string) error {
		trip, route := row["trip_id"], row["route_id"]
		if trip == "" {
			return nil
		}
		offset, ok := index.byRoute[route]
		if !ok {
			// A trip that names a route which routes.txt does not hold is a
			// broken dataset. Skip it rather than inventing a route.
			return nil
		}
		index.byTrip[trip] = offset
		return nil
	})
}

func readFeedInfo(reader *zip.Reader) FeedInfo {
	var info FeedInfo
	_ = rows(reader, "feed_info.txt", false, func(row map[string]string) error {
		if info.Version == "" && info.StartDate == "" && info.EndDate == "" {
			info = FeedInfo{
				Version:   row["feed_version"],
				StartDate: row["feed_start_date"],
				EndDate:   row["feed_end_date"],
			}
		}
		return nil
	})
	return info
}

// bomStripper removes a leading byte order mark. Several French feeds write
// one, and it would otherwise become part of the first column name.
type bomStripper struct {
	reader  io.Reader
	checked bool
	pending []byte
}

func (b *bomStripper) Read(p []byte) (int, error) {
	if !b.checked {
		b.checked = true
		head := make([]byte, 3)
		n, err := io.ReadFull(b.reader, head)
		head = head[:n]
		if string(head) != "\xef\xbb\xbf" {
			b.pending = head
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return 0, err
		}
	}
	if len(b.pending) > 0 {
		copied := copy(p, b.pending)
		b.pending = b.pending[copied:]
		return copied, nil
	}
	return b.reader.Read(p)
}
