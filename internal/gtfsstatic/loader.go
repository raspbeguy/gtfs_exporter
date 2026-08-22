package gtfsstatic

import (
	"archive/zip"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMaxBytes limits the size of a static feed. Ile-de-France is 271 MB,
// so the limit leaves room for a larger one.
const DefaultMaxBytes = 512 << 20

// DefaultRefreshInterval is the gap between two loads. A conditional request
// makes most of them cost one round trip.
const DefaultRefreshInterval = 12 * time.Hour

// DefaultRenewBefore starts the renewal margin two days before a dataset
// expires. Rennes publishes a twelve day window, so two days is a comfortable
// fraction of it.
const DefaultRenewBefore = 48 * time.Hour

// DefaultRenewInterval is the retry gap inside the margin. A conditional
// request answers 304, so an hourly attempt is cheap.
const DefaultRenewInterval = time.Hour

// Feed names one static GTFS feed.
type Feed struct {
	URL             string
	Headers         map[string]string
	RefreshInterval time.Duration
	MaxBytes        int64
	// Schedule asks for the timetable of stop_times.txt. It is off by default,
	// because that member is the largest of the zip and the result stays in
	// memory: about 19 MB for Rennes and 39 MB for MBTA. Turn it on for a feed
	// that publishes no delay of its own.
	Schedule bool
	// MaxScheduleRows refuses a network whose timetable would not fit.
	MaxScheduleRows int
	// RenewBefore starts renewing this long before the dataset expires.
	RenewBefore time.Duration
	// RenewInterval is the retry gap inside that margin.
	RenewInterval time.Duration
}

// Loader keeps one index up to date in the background.
//
// A scrape never waits for a load. Reading and parsing Ile-de-France takes
// about fifteen seconds, and no scrape can carry that.
type Loader struct {
	feed   Feed
	client *http.Client
	log    *slog.Logger

	// index is replaced whole. An Index is immutable, so a reader takes one
	// atomic load and needs no lock.
	index atomic.Pointer[Index]
	// pending holds a dataset that has been read but has not started. See the
	// comment in once.
	pending atomic.Pointer[Index]
	// failures counts the loads that did not finish. A failed load keeps the
	// index that is already in place.
	failures atomic.Int64
	// clock is nil outside a test.
	clock func() time.Time
	etag  atomic.Pointer[string]

	start sync.Once
	stop  chan struct{}
}

// NewLoader returns a loader for one feed. Call Start once to begin.
func NewLoader(feed Feed, client *http.Client, log *slog.Logger) *Loader {
	if feed.RefreshInterval <= 0 {
		feed.RefreshInterval = DefaultRefreshInterval
	}
	if feed.MaxBytes <= 0 {
		feed.MaxBytes = DefaultMaxBytes
	}
	if feed.MaxScheduleRows <= 0 {
		feed.MaxScheduleRows = DefaultMaxScheduleRows
	}
	if feed.RenewBefore <= 0 {
		feed.RenewBefore = DefaultRenewBefore
	}
	if feed.RenewInterval <= 0 {
		feed.RenewInterval = DefaultRenewInterval
	}
	return &Loader{feed: feed, client: client, log: log, stop: make(chan struct{})}
}

// Index returns the index that is ready now.
//
// It returns nil while the first load runs, and also for a nil loader, which
// is what a module without a static feed holds. A caller can then treat "no
// static feed" and "not loaded yet" the same way.
func (l *Loader) Index() *Index {
	if l == nil {
		return nil
	}
	return l.index.Load()
}

// Pending reports whether a newer dataset is held back until its start date.
func (l *Loader) Pending() bool {
	return l != nil && l.pending.Load() != nil
}

// Failures returns the number of loads that did not finish.
func (l *Loader) Failures() float64 {
	if l == nil {
		return 0
	}
	return float64(l.failures.Load())
}

// URL returns the address of the feed.
func (l *Loader) URL() string {
	if l == nil {
		return ""
	}
	return l.feed.URL
}

// Start begins the background loads. It returns at once, and the first index
// appears later.
func (l *Loader) Start(ctx context.Context) {
	l.start.Do(func() {
		go l.run(ctx)
	})
}

// Stop ends the background loads.
func (l *Loader) Stop() { close(l.stop) }

func (l *Loader) run(ctx context.Context) {
	for {
		l.once(ctx)
		timer := time.NewTimer(l.nextRefresh(l.now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-l.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// nextRefresh returns the wait before the next load.
//
// The configured interval is a floor, not the whole policy, because the life
// of a dataset varies by a factor of twenty: Rennes publishes a twelve day
// window and SNCF a one hundred and sixty two day one. At a flat interval the
// long feed reloads for nothing while the short one turns over between checks.
//
// So the data supplies a deadline. Inside the renewal margin the loader polls
// at the shorter interval until a later dataset arrives, and each attempt is
// one conditional request that answers 304.
func (l *Loader) nextRefresh(now time.Time) time.Duration {
	wait := l.feed.RefreshInterval

	index := l.index.Load()
	expires, known := index.ExpiresAt()
	if !known {
		return wait
	}
	renewFrom := expires.Add(-l.feed.RenewBefore)
	if !now.Before(renewFrom) {
		// The dataset is inside its margin, or already past it.
		return l.feed.RenewInterval
	}
	if until := renewFrom.Sub(now); until < wait {
		wait = until
	}
	return wait
}

// now reads the clock. A test replaces it to drive the renewal policy without
// waiting.
func (l *Loader) now() time.Time {
	if l.clock != nil {
		return l.clock()
	}
	return time.Now()
}

// once runs one load.
//
// A load that fails keeps the index that is already in place. A stale route
// name is better than none, and a feed that answers badly for an hour must not
// empty the labels of every metric.
func (l *Loader) once(ctx context.Context) {
	// Promote a dataset that was held back until its start date. This runs
	// before the fetch, so a promotion costs no request at all.
	if held := l.pending.Load(); held != nil {
		if start, ok := held.StartsAt(); ok && !l.now().Before(start) {
			l.log.Info("promoting the dataset that was held", "url", l.feed.URL)
			l.install(held)
			return
		}
	}

	// The budget is generous, because the work is off the scrape path.
	loadCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	index, err := l.load(loadCtx)
	switch {
	case errors.Is(err, ErrNotModified):
		l.log.Debug("the static feed has not changed", "url", l.feed.URL)
		return
	case err != nil:
		l.failures.Add(1)
		l.log.Warn("cannot load the static feed, keeping the index that is in place",
			"url", l.feed.URL, "error", err, "loaded", l.index.Load() != nil)
		return
	}
	// A dataset that has not started yet must not replace one that is correct
	// today. A producer publishes the next timetable before it takes effect,
	// and installing it early would answer with a service that does not run.
	//
	// Holding it and refetching later does not work either: the ETag would be
	// unchanged, the request would answer 304, and the promotion would never
	// happen. So keep the parsed index and promote it when its day arrives.
	if start, ok := index.StartsAt(); ok && l.now().Before(start) {
		if current := l.index.Load(); current != nil && current.InService(l.now()) {
			l.pending.Store(index)
			l.log.Info("holding a dataset that has not started yet",
				"url", l.feed.URL, "starts", start.Format(time.DateOnly))
			return
		}
	}
	l.install(index)
}

// install replaces the current index and clears any pending one.
func (l *Loader) install(index *Index) {
	l.index.Store(index)
	l.pending.Store(nil)
	agencies, routes, trips := index.Stats()
	l.log.Info("loaded the static feed", "url", l.feed.URL,
		"agencies", agencies, "routes", routes, "trips", trips,
		"bytes", index.Bytes(), "seconds", index.Took().Seconds(),
		"feed_version", index.FeedInfo().Version,
		"schedule_rows", index.Schedule().Rows())
}

func (l *Loader) load(ctx context.Context) (*Index, error) {
	started := time.Now()

	previous := ""
	if tag := l.etag.Load(); tag != nil {
		previous = *tag
	}
	src, err := open(ctx, l.client, l.feed.URL, l.feed.Headers, previous, l.feed.MaxBytes)
	if err != nil {
		return nil, err
	}
	defer src.close()

	reader, err := zip.NewReader(src.readerAt, src.size)
	if err != nil {
		return nil, err
	}
	index, err := build(reader, l.feed.Schedule, l.feed.MaxScheduleRows)
	// A schedule that failed to build leaves a usable route index behind, so
	// install it and warn rather than keeping a older one that is no better.
	var scheduleErr *ScheduleError
	if errors.As(err, &scheduleErr) {
		l.log.Warn("cannot build the timetable, keeping the routes without it",
			"url", l.feed.URL, "error", scheduleErr.Err)
	} else if err != nil {
		return nil, err
	}
	// Count the bytes and the time only now. build is what reads the members,
	// so a counter sampled before it would report the tail request alone.
	index.bytes = src.read.Load()
	index.took = time.Since(started)
	// Store the validator only after a load that worked. A tag stored beside a
	// failed parse would make every later refresh answer 304 and never repair
	// the index.
	tag := src.etag
	l.etag.Store(&tag)
	return index, nil
}

// Registry holds one loader per static feed.
//
// Three realtime feeds of one network name the same static feed, so they must
// share one loader and one index rather than fetching 271 MB three times.
type Registry struct {
	mutex   sync.Mutex
	loaders map[string]*Loader
	client  *http.Client
	log     *slog.Logger
	ctx     context.Context
}

// NewRegistry returns a registry. The context ends every loader.
func NewRegistry(ctx context.Context, client *http.Client, log *slog.Logger) *Registry {
	return &Registry{loaders: map[string]*Loader{}, client: client, log: log, ctx: ctx}
}

// Loader returns the loader of the feed and starts it on the first call. The
// key is the URL of the feed.
func (r *Registry) Loader(feed Feed) *Loader {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	key := loaderKey(feed)
	if loader, ok := r.loaders[key]; ok {
		return loader
	}
	loader := NewLoader(feed, r.client, r.log)
	r.loaders[key] = loader
	loader.Start(r.ctx)
	return loader
}

// loaderKey names the sharing group of a feed.
//
// The URL alone is not enough. Two modules can name the same zip and ask for
// different things from it, and one of them would then silently receive the
// index that the other asked for: a module with schedule set would get an
// index with no timetable, and publish no delay for a feed that should have
// one. The flag therefore joins the key, and the two modules keep one index
// each.
//
// Everything else about a feed only changes how the zip is read, not what the
// index holds, so it stays out of the key.
func loaderKey(feed Feed) string {
	if feed.Schedule {
		return feed.URL + "\x00schedule"
	}
	return feed.URL
}

// NextRefreshForTest exposes the renewal policy to the tests of this package.
func NextRefreshForTest(index *Index, now time.Time, interval, before, retry time.Duration) time.Duration {
	loader := &Loader{feed: Feed{RefreshInterval: interval, RenewBefore: before, RenewInterval: retry}}
	loader.index.Store(index)
	return loader.nextRefresh(now)
}
