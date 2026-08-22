# gtfs_exporter

[![CI](https://github.com/raspbeguy/gtfs_exporter/actions/workflows/ci.yml/badge.svg)](https://github.com/raspbeguy/gtfs_exporter/actions/workflows/ci.yml)

A Prometheus exporter for GTFS-Realtime, the standard that public transport
agencies use to publish live data.

A GTFS-Realtime feed is a protobuf payload over HTTP. It holds trip updates,
vehicle positions, and service alerts. The exporter answers two questions:

1. Is the service running normally? Count the vehicles, the delays, the
   cancelled trips, and the alerts.
2. Is the feed itself healthy? Report the age of the feed, the entity counts,
   and which fields the producer leaves out.

The second question is the reason for a large part of this exporter. Producers
omit fields constantly, and they omit them for some entities and not for
others.

The exporter follows the multi-target pattern of blackbox_exporter. One scrape
of `/probe` reads one feed URL, and the configuration file holds no target.

## Warning

Two settings can hurt you. Read this before you deploy.

Match the scrape interval to the producer. GTFS-Realtime has no `ttl` field, so
the exporter cannot advertise the right interval. Cadence in the wild varies by
a factor of ten between the feeds measured, from a new header timestamp every
10 seconds to one every 94. Watch `gtfs_rt_feed_timestamp_seconds` and set the
interval from what you see.

Count the routes of a network before you set `per_route`. A city network has
tens of routes, a large metropolitan one has thousands.

## What the exporter reads

One target is one feed URL. GTFS-Realtime has no discovery document, so a
network that publishes trip updates, vehicle positions and service alerts
publishes three URLs and gets three targets. Some producers put all three kinds
of entity in one URL, and the exporter then reports all three families from
that one target.

The exporter reads the entity kinds of the current specification: trip updates,
vehicle positions, alerts, shapes, stops, and trip modifications. It counts the
last three and reports nothing else about them, because almost no producer
publishes them.

A module can also name the **static** GTFS feed of the network. The exporter
then resolves the route of each trip, which a realtime feed often leaves out.
See [Static GTFS](#static-gtfs).

## Install

### Container image

```sh
docker run -p 10053:10053 -v ./config.yml:/etc/gtfs_exporter/config.yml:ro \
  ghcr.io/raspbeguy/gtfs_exporter:latest
```

The image ships a default configuration at `/etc/gtfs_exporter/config.yml`. It
sets no `allowed_hosts`, so the exporter warns at start. Mount your own file
over that path for a real deployment.

### Binary

Download the archive for your platform from the releases page, then verify it:

```sh
sha256sum -c checksums.txt --ignore-missing
tar xzf gtfs_exporter_*_linux_amd64.tar.gz
./gtfs_exporter -config config.yml
```

## Build

Go 1.25 or higher is necessary.

```sh
make build
```

## Configure

1. Copy `config.example.yml` to `config.yml`.
2. Set `allowed_hosts` to the hosts that you scrape.
3. Add a module for each operator that needs one.

The file rejects an unknown key, so a typo stops the exporter at start rather
than dropping a setting in silence.

### Warning

The `/probe` endpoint fetches the URL that the caller gives. A caller who
reaches this port can read any address that the exporter can reach. Set
`allowed_hosts`, or keep the port closed to untrusted callers.

The host list is tested again on every redirect. A target that answers with a
redirect into your network is refused, and the refusal is counted under
`gtfs_exporter_requests_rejected_total{reason="forbidden_redirect"}`.

The `static_gtfs` URL of a module skips the host test, because it comes from
the configuration file and not from a caller. This matters in practice: the
a static feed is commonly served through a redirect to a third-party host.

### Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `listen_address` | `:10053` | Address that the exporter listens on. |
| `timeout` | `30s` | Budget for one scrape. |
| `request_timeout` | `10s` | Budget for the HTTP read of one feed. |
| `user_agent` | `gtfs_exporter/<version>` | User agent of every request. |
| `allowed_hosts` | empty | Hosts accepted as a target. Empty accepts every host. |
| `max_in_flight` | `4` | Scrapes that may run at the same time. |
| `modules` | empty | Read settings, by name. |

Prometheus sends `X-Prometheus-Scrape-Timeout-Seconds` on every scrape, and the
exporter uses the lower of that value and `timeout`. A budget above the scrape
timeout only spends work on a request that Prometheus has already abandoned.

### Modules

| Setting | Default | Meaning |
| --- | --- | --- |
| `per_route` | `false` | Add one series per route to the trip-update family. |
| `headers` | empty | Headers of every request. Use them for an API key. |
| `request_timeout` | file value | Budget for the feed of this operator. |
| `reuse_connections` | `true` | Keep the connection alive between scrapes. |
| `delay_buckets` | see below | Classic histogram bounds of the trip delay, in seconds. |
| `native_histogram` | `false` | Publish the delay as a native histogram. |
| `static_gtfs` | none | The static GTFS feed of this network. |

A request that names no module gets the module called `default`.

## Static GTFS

A realtime feed often names a trip and no route. **One feed measured publishes
1140 trip updates and sets `route_id` on none of them**, and it is not alone.
Without the static feed the exporter counts the trips but names no route.

Give the module a `static_gtfs` URL and the exporter resolves the route of each
trip. On that feed it turns zero named routes into 460, with their short names
and their types.

```yaml
modules:
  my-network:
    per_route: true
    static_gtfs:
      url: https://example.org/gtfs/static.zip
      refresh_interval: 12h
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `url` | none | Address of the static GTFS zip. |
| `headers` | empty | Headers of the request. |
| `refresh_interval` | `12h` | Gap between two loads. |
| `max_bytes` | 512 MB | Size limit of the zip. |

Four rules govern the loader.

**It reads the small members only, unless you ask for a timetable.**
`agency.txt`, `routes.txt`, `trips.txt`, `feed_info.txt` and the calendar. It
never reads `shapes.txt`, which reaches 991 MB uncompressed on a large
dataset, and
it reads `stop_times.txt` only when a module sets `schedule`. See
[The timetable](#the-timetable).

**It uses HTTP range requests.** On the largest dataset measured, a 271 MB
zip, the exporter reads 4.1 MB of it, which is 1.5%. A server that does not answer a
range request gets a full download instead. A conditional request with the
stored `ETag` skips a refresh whose dataset has not changed.

**A scrape never waits for a load.** Reading a large dataset takes about 30
seconds. The load runs in the background, and until the first one finishes
`gtfs_static_loaded` is 0 and every realtime metric still appears.

**A failed refresh keeps the index that is in place.** A stale route name is
better than none.

The three realtime feeds of one network must name the same `static_gtfs` URL.
The exporter then keeps one index for all of them.

## The timetable

Set `schedule: true` on a module and the loader also reads `stop_times.txt`.
The exporter can then measure a delay for a feed that publishes only absolute
times, which phase 1 could not.

**Turn it on only for a feed that needs it.** Four of the six trip-update feeds
measured already publish a delay of their own, and for those the timetable buys
nothing at the price of the largest member of the zip.

| Network | `stop_times` rows | Live heap | Resident | Delay before | Delay now |
| --- | --- | --- | --- | --- | --- |
| A city network | 1,004,256 | 34 MB | 65 MB | none | median 0 s |
| A metropolitan network | 2,221,062 | 63 MB | 105 MB | none | median 0 s |
| A national rail network | 394,083 | 35 MB | 65 MB | already had one | unchanged |

Measured, not estimated. Budget about **30 to 40 bytes per stop-time row**, plus
a fixed cost for the trip and route index that dominates a small timetable. The
third row holds 45,610 trips behind only 394,083 stop times, which is why its
cost per row looks high.

Resident memory runs near twice the live heap at the default `GOGC`, because Go
returns pages to the system lazily. Set `GOGC=20` to trade processor time for a
smaller footprint.

`schedule_max_rows` refuses a network whose timetable would not fit, and the
route index still loads. Watch `gtfs_static_schedule_rows`: no series at all
means the module asked for no timetable, and 0 means it asked and did not get
one.

### Renewal

A dataset expires quickly, and the horizon varies by a factor of twenty across
the feeds measured, from a twelve day window to a hundred and sixty two. So
`refresh_interval` is a floor, and the expiry date in the data supplies a
deadline on top:

```
next = min(now + refresh_interval, expires - renew_before)
inside that margin: next = now + renew_interval
```

The expiry comes from `feed_end_date`, and from the widest window of
`calendar.txt` and `calendar_dates.txt` when the feed states none. Both are
optional in GTFS, and the fallback is not theoretical: a feed may state no
window at all and express its whole service through `calendar_dates.txt`.

**A dataset outside its window stops the delay.** A timetable that has expired
describes a service that no longer runs, so a delay measured against it is
wrong rather than missing, which is worse. The route resolution survives,
because a route name ages gracefully. Alert on `gtfs_static_in_service`, and
watch `gtfs_static_expires_timestamp_seconds` well before that.

A dataset that has not started yet is held back rather than installed, so a
timetable that is correct today is never replaced by one that is not. While
that happens, `gtfs_static_pending` is 1.

### What the static feed still cannot fix

- The route of an alert that names a stop. An alert that names a trip resolves
  through the timetable, **when the two feeds agree on the identifier**. One
  feed measured does not: its alerts write a short trip identifier while
  `trips.txt` writes a long composite one, so none of its 7,231 alert trips
  matches. Those are counted under
  `gtfs_rt_alerts_unresolved{scope="unresolved_trip"}` rather than guessed at.
  A feed that names routes directly resolves cleanly.
- A blank time at a stop that is not a timepoint. GTFS expects the reader to
  interpolate between the surrounding timepoints, and this exporter does not:
  it skips the row. A feed that marks only its timepoints therefore yields
  fewer observations than it could.
- A frequency-based trip. Its stop times count from the start of the trip and
  not from the service day, so the exporter reports no delay for it at all.

## Run

| Flag | Default | Meaning |
| --- | --- | --- |
| `-config` | `config.yml` | Path of the configuration file. |
| `-web.listen-address` | from the file | Address to listen on. |
| `-log.level` | `info` | `debug`, `info`, `warn`, or `error`. |
| `-version` | | Print the version and exit. |

| Endpoint | Content |
| --- | --- |
| `/probe` | Metrics of the feed that the query string names. |
| `/metrics` | Metrics of the exporter itself. |
| `/healthz` | `ok`. |
| `/` | A landing page. |

| Parameter | Necessary | Meaning |
| --- | --- | --- |
| `target` | yes | URL of the GTFS-Realtime feed. |
| `module` | no | Name of a module. The default is `default`. |

```sh
curl 'localhost:10053/probe?target=https://example.org/gtfs-rt/vehicle-positions.pb'
```

### The network label

The exporter sets no `network` label and no `system` label. One scrape reads
one feed, and Prometheus owns the identity of a target. Set the label with
`relabel_configs`, as the example below does.

## Metrics

### Feed health

| Metric | Labels | Meaning |
| --- | --- | --- |
| `gtfs_rt_up` | | 1 if the exporter fetched and decoded the feed. Always present. |
| `gtfs_rt_feed_timestamp_seconds` | | Header time, exactly as the producer wrote it. |
| `gtfs_rt_fetch_duration_seconds` | | Seconds to fetch and decode the feed. |
| `gtfs_rt_feed_size_bytes` | | Size of the payload. |
| `gtfs_rt_feed_info` | `gtfs_realtime_version`, `incrementality` | Always 1. |
| `gtfs_rt_entities` | `entity_type` | Entities by kind. |

### Trip updates

| Metric | Labels | Meaning |
| --- | --- | --- |
| `gtfs_rt_trip_updates` | `schedule_relationship` | Trips by relationship. `CANCELED` is the cancellation count. |
| `gtfs_rt_stop_time_updates` | `schedule_relationship` | Stop time updates. `SKIPPED` is the skipped-stop count. |
| `gtfs_rt_trip_delay_seconds` | | Histogram. One observation per trip. See [Delay](#delay). |
| `gtfs_rt_route_trip_updates` | `route_id` | Trips per route. Only with `per_route`. |
| `gtfs_rt_route_trip_delay_seconds` | `route_id` | Histogram per route. Only with `per_route_delay`. A sum over it equals `gtfs_rt_trip_delay_seconds`. |
| `gtfs_rt_route_trips_late` | `route_id`, `threshold` | Trips past `late_threshold`. |
| `gtfs_rt_route_trips_cancelled` | `route_id` | Cancelled trips. A route with none gets no series. |
| `gtfs_rt_route_vehicles` | `route_id` | Vehicles on the route. |
| `gtfs_rt_route_alerts` | `route_id`, `effect` | Alerts in force naming the route, counted once per route and not per selector. |
| `gtfs_rt_alerts_unresolved` | `scope` | Alerts that name no route: `agency`, `route_type`, `stop`, `unresolved_trip`, `none`. |
| `gtfs_rt_delay_source_info` | `source` | Always 1. `producer` or `computed`. |
| `gtfs_rt_trips_without_schedule` | `reason` | Trips the timetable cannot time. Split the label before alerting. |

### Vehicle positions

| Metric | Labels | Meaning |
| --- | --- | --- |
| `gtfs_rt_vehicles` | `current_status` | Vehicles by stop status. Sum it for the vehicle count. |
| `gtfs_rt_vehicle_occupancy` | `occupancy_status` | Vehicles by occupancy. Never add it to `gtfs_rt_vehicles`. |
| `gtfs_rt_vehicle_congestion` | `congestion_level` | Vehicles by congestion. Never add it to `gtfs_rt_vehicles`. |
| `gtfs_rt_vehicle_timestamp_age_seconds` | | Histogram of the age of each vehicle report. |

Occupancy and congestion are separate metrics because most feeds omit them.
Only 2 of 18 vehicle feeds measured publish occupancy at all. GTFS-Realtime has
a real `NO_DATA_AVAILABLE` occupancy value, so a label cannot say "the producer
omitted this" without losing that meaning.

### Alerts

| Metric | Labels | Meaning |
| --- | --- | --- |
| `gtfs_rt_alerts` | `cause`, `effect`, `severity_level`, `active` | Alerts, and whether each is in force now. |
| `gtfs_rt_alert_informed_entities` | `selector` | Selector **fields**, by field name. |

Never add the `gtfs_rt_alert_informed_entities` series together. One selector
can set several fields at once: a producer commonly sets `route_id`,
`route_type` and `direction_id` on the same one.

The exporter reads `impact_period` first, then `communication_period`, then the
deprecated `active_period`. An alert with no period at all is active, because
the specification shows it for as long as it appears in the feed.

### Feed quality

| Metric | Labels | Meaning |
| --- | --- | --- |
| `gtfs_rt_entities_with_field` | `entity_type`, `field` | Entities that carry an optional field. |
| `gtfs_rt_duplicate_entity_ids` | | Identifiers that the feed uses more than once. |
| `gtfs_rt_entities_without_payload` | | Entities that set no payload field. |
| `gtfs_rt_entities_deleted` | | Entities that set `is_deleted` on a `FULL_DATASET` feed. |

`gtfs_rt_entities_with_field` is a count and not a ratio. Coverage inside one
feed is partial. Among the feeds measured, one publishes occupancy for 84 of
its 85 vehicles, another for 355 of its 473, and a third publishes `route_id`
for 52 of its 54. Divide by `gtfs_rt_entities` when you want a ratio.

### Static index

| Metric | Labels | Meaning |
| --- | --- | --- |
| `gtfs_static_loaded` | | 1 when an index is ready, 0 while the first load runs. |
| `gtfs_static_info` | `feed_version`, `feed_start_date`, `feed_end_date` | Always 1. Names the dataset. |
| `gtfs_static_last_load_timestamp_seconds` | | When the index was built. |
| `gtfs_static_load_duration_seconds` | | Seconds that the last load took. |
| `gtfs_static_bytes_fetched` | | Bytes of the zip that the last load read. |
| `gtfs_static_agencies` | | Agencies in the index. |
| `gtfs_static_routes` | | Routes in the index. |
| `gtfs_static_trips` | | Trips in the index. |
| `gtfs_static_route_info` | `route_id`, `route_short_name`, `route_type`, `agency_id` | Always 1. Join it on `route_id`. |
| `gtfs_rt_trips_not_in_static` | | Realtime trips that the dataset does not hold. |
| `gtfs_static_schedule_rows` | | Stop times in the timetable. No series when `schedule` is off, 0 when it is on and there is none. |
| `gtfs_static_in_service` | | 1 when today falls inside the validity window. The delay stops while this is 0. |
| `gtfs_static_expires_timestamp_seconds` | | When the dataset stops describing the service. |
| `gtfs_static_pending` | | 1 while a newer dataset is held until its start date. |

`gtfs_rt_trips_not_in_static` and `gtfs_rt_trips_without_schedule` overlap but
differ. The first counts trips with no **route**, and it exists whether or not
the timetable is on. The second counts trips with no usable **timetable entry**.
They agree for a healthy feed and diverge for a broken one. Never add them.

These series repeat under every target that shares one static feed. That is
deliberate. A reader who looks at one target must see whether the index that
fills its labels is ready.

`gtfs_static_route_info` appears only for the routes that the realtime feed
names, and not for every route of the network. `gtfs_rt_route_trip_updates`
carries the identifier alone, so join the two:

```promql
gtfs_rt_route_trip_updates
  * on (instance, route_id) group_left (route_short_name, route_type)
  gtfs_static_route_info
```

`gtfs_rt_trips_not_in_static` leaves out the trips that have no static row by
design: those marked `ADDED`, `NEW` or `DUPLICATED`, and those that carry no
`trip_id`. Expect a spike on the day that a producer rolls a new dataset, and
read `gtfs_static_info` before you believe the number.

### The exporter itself

`/metrics` carries `gtfs_exporter_build_info`, the Go and process collectors,
and `gtfs_exporter_requests_rejected_total{reason}` with the reasons
`bad_target`, `forbidden_host`, `forbidden_redirect`, `unknown_module`, and
`too_many_in_flight`.

## Absent, not zero

A field that the producer omits gets **no series**. A zero would claim a
reading that nobody made: an empty vehicle, a punctual trip, a feed from 1970.

Alert on `gtfs_rt_up`, and never on a series that disappears. A metric that
vanishes cannot fire an alert.

## Delay

**A delay is not always possible, and this is the most common surprise.**

Of the six trip-update feeds measured, two publish no delay at all. One sends
7843 stop time updates with absolute times only, another 22,642 the same way.
For those feeds, set `schedule: true` on the module and the
exporter measures the predictions against the timetable. See
[The timetable](#the-timetable).

Without a timetable, `gtfs_rt_trip_delay_seconds` is absent for such a feed.
That is the correct answer, and not a fault.

`gtfs_rt_delay_source_info` says which kind of number a target carries:
`producer` is what the operator published, and `computed` is what this exporter
derived. The second is the weaker reading.

The exporter takes one observation per trip, in this order:

1. `TripUpdate.delay`, the trip-level field. Only one feed measured sets it.
2. The `delay` of the first stop time update that the vehicle has not passed,
   preferring `departure` over `arrival`. Most producers use this.
3. The difference between `time` and `scheduled_time` on a stop time event.
   This is the newer form of the specification, and it is the only way that a
   feed can state a delay with no static schedule. No feed measured sets it
   yet.
4. The predicted time minus the scheduled time from the timetable, when the
   module set `schedule`. This is the only source for a feed that publishes
   absolute times alone. The join uses `stop_sequence` when the producer sends
   one, and `stop_id` otherwise, because some producers send no sequence.
5. Nothing.

A cancelled trip never contributes. A stop marked `SKIPPED` or `NO_DATA` never
contributes, because `NO_DATA` states that the producer has no prediction.

`gtfs_rt_trips_without_schedule` says why a trip produced no delay, by reason.
**Several reasons are healthy.** A producer commonly sends a short update that
skips a stop or two on a trip which is still running: every stop it carries is
`SKIPPED`, so none can be timed, and those appear under `no_timeable_stop`. On
one feed measured that is about 40% of its trip updates. Never alert on the
total without splitting the label.

The default buckets cover -600 to 7200 seconds. Measured delays run from -493
seconds early to 7800 seconds late.

### Native histograms

Set `native_histogram: true` in a module to publish the delay as a native
histogram. Read this first.

A constant native histogram carries **no** classic buckets. One metric is one
form or the other. A Prometheus without native-histogram support then sees only
`_sum` and `_count`. It needs `--enable-feature=native-histograms` and protobuf
in `scrape_protocols`.

Native buckets are exponential, and the median delay of a real feed is exactly
0. The resolution is therefore worst where most of the data sits. The classic
form is the better default for delays.

## Stale data

GTFS-Realtime has no `ttl` field, so a stale feed still answers HTTP 200. Watch
the header time instead:

```promql
time() - gtfs_rt_feed_timestamp_seconds > 300
```

Four of thirty feeds measured publish a header time **in the future**, by up to
77 seconds. The exporter never clamps the value, so this expression goes
negative for them. Alert on the upper bound alone.

## Prometheus configuration

```yaml
scrape_configs:
  - job_name: gtfs_rt
    scrape_interval: 30s
    metrics_path: /probe
    static_configs:
      - targets:
          - https://example.org/gtfs-rt/trip-updates.pb
          - https://example.org/gtfs-rt/vehicle-positions.pb
          - https://example.org/gtfs-rt/alerts.pb
        labels:
          network: my-network
    params:
      module: [my-network]
    relabel_configs:
      - source_labels: [__address__]
        target_label: __param_target
      - source_labels: [__param_target]
        target_label: instance
      - target_label: __address__
        replacement: gtfs-exporter:10053

  # The exporter itself.
  - job_name: gtfs_exporter
    static_configs:
      - targets: ["gtfs-exporter:10053"]
```

Give each network its own job, or set the `network` label with
`static_configs`, so that the three feeds of one network group together.

## Example queries

Feeds that stopped updating:

```promql
time() - gtfs_rt_feed_timestamp_seconds > 300
```

Share of trips more than five minutes late:

```promql
1 - (
  sum by (instance) (gtfs_rt_trip_delay_seconds_bucket{le=~"300(\\.0)?"})
  / sum by (instance) (gtfs_rt_trip_delay_seconds_count)
)
```

The `le` matcher is a regular expression on purpose. A bucket bound reaches the
database as `300` over the text exposition format and as `300.0` over the
protobuf one, and Grafana Alloy negotiates protobuf. An exact match on either
spelling silently returns nothing against the other.

Producers that dropped a field they used to publish:

```promql
gtfs_rt_entities_with_field{field="route_id"}
  / on (instance) group_left sum by (instance) (gtfs_rt_entities)
```

Cancelled trips as a share of the service:

```promql
gtfs_rt_trip_updates{schedule_relationship="CANCELED"}
  / ignoring (schedule_relationship)
    sum without (schedule_relationship) (gtfs_rt_trip_updates)
```

Alerts in force now, by severity:

```promql
sum by (severity_level) (gtfs_rt_alerts{active="true"})
```

Static index that failed to refresh:

```promql
time() - gtfs_static_last_load_timestamp_seconds > 86400
```

## Grafana dashboards

The `grafana` directory holds two dashboards. Import them, or provision them
with `grafana/provisioning-dashboard.yml`.

| Dashboard | Content |
| --- | --- |
| `gtfs-rt-service.json` | Vehicles, delays, cancellations, alerts. |
| `gtfs-rt-health.json` | Feed age, entity counts, field coverage, static index. |

## Test

```sh
make lint
go test -race ./...
```

No test reads a real operator. The fixtures are built in memory: a
GTFS-Realtime payload with `proto.Marshal`, and a static GTFS zip with
`archive/zip`.

## License

MIT. See [LICENSE](LICENSE).
