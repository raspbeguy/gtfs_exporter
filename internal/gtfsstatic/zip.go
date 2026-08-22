package gtfsstatic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

// blockSize is the unit that the range reader fetches and caches.
//
// archive/zip walks the central directory with many small reads. Without a
// block, each one would cost a request.
const blockSize = 64 << 10

// tailSize is the first read of a remote zip. It must hold the
// end-of-central-directory record and, for most feeds, the whole central
// directory. Ile-de-France holds 15 members in 899 bytes.
const tailSize = 64 << 10

// errNoRange reports that the server does not answer a range request.
var errNoRange = errors.New("gtfsstatic: the server does not support range requests")

// ErrNotModified reports that the feed has not changed since the last load.
var ErrNotModified = errors.New("gtfsstatic: the feed has not changed")

// source is a remote or local file that archive/zip can read.
type source struct {
	readerAt io.ReaderAt
	size     int64
	// etag is the validator of this reading, for the next conditional request.
	etag  string
	read  *atomic.Int64
	close func()
}

// open prepares a zip for reading.
//
// It first asks for the tail of the file with a range request. A server that
// answers 206 lets archive/zip read the central directory and one member
// without the exporter holding the whole file: Ile-de-France is 271 MB, of
// which the exporter needs 3.96 MB.
//
// A server that ignores the range header sends the whole body. The body then
// goes to a temporary file, because archive/zip needs an io.ReaderAt and a
// stream is not one.
func open(ctx context.Context, client *http.Client, url string, headers map[string]string,
	etag string, maxBytes int64,
) (*source, error) {
	read := &atomic.Int64{}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("gtfsstatic: cannot build a request for %s: %w", url, err)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	request.Header.Set("Range", fmt.Sprintf("bytes=-%d", tailSize))
	// A conditional request skips the whole load when the dataset has not
	// changed. The static feed of data.gouv.fr serves an ETag, so a refresh
	// every twelve hours costs one request on most days.
	if etag != "" {
		request.Header.Set("If-None-Match", etag)
	}

	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("gtfsstatic: cannot fetch %s: %w", url, err)
	}

	switch response.StatusCode {
	case http.StatusNotModified:
		_ = response.Body.Close()
		return nil, ErrNotModified

	case http.StatusPartialContent:
		total, start, err := parseContentRange(response.Header.Get("Content-Range"))
		if err != nil {
			_ = response.Body.Close()
			return nil, err
		}
		if total > maxBytes {
			_ = response.Body.Close()
			return nil, fmt.Errorf("gtfsstatic: %s is %d bytes, above the limit of %d", url, total, maxBytes)
		}
		tail, err := io.ReadAll(io.LimitReader(response.Body, tailSize+1))
		_ = response.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("gtfsstatic: cannot read the tail of %s: %w", url, err)
		}
		read.Add(int64(len(tail)))
		reader := &rangeReaderAt{
			ctx: ctx, client: client, url: url, headers: headers,
			size: total, read: read,
			cacheStart: start, cache: tail,
		}
		return &source{
			readerAt: reader, size: total, etag: response.Header.Get("ETag"),
			read: read, close: func() {},
		}, nil

	case http.StatusOK:
		// The server ignored the range header and sent the whole file.
		defer func() { _ = response.Body.Close() }()
		file, size, err := spill(response.Body, maxBytes)
		if err != nil {
			return nil, fmt.Errorf("gtfsstatic: cannot buffer %s: %w", url, err)
		}
		read.Add(size)
		return &source{
			readerAt: file, size: size, etag: response.Header.Get("ETag"),
			read: read,
			close: func() {
				name := file.Name()
				_ = file.Close()
				_ = os.Remove(name)
			},
		}, nil

	default:
		_ = response.Body.Close()
		return nil, fmt.Errorf("gtfsstatic: %s returned HTTP %d", url, response.StatusCode)
	}
}

// spill writes a body to a temporary file, so that archive/zip can seek in it.
func spill(body io.Reader, maxBytes int64) (*os.File, int64, error) {
	file, err := os.CreateTemp("", "gtfs-static-*.zip")
	if err != nil {
		return nil, 0, err
	}
	size, err := io.Copy(file, io.LimitReader(body, maxBytes+1))
	if err != nil || size > maxBytes {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
		if err == nil {
			err = fmt.Errorf("the feed is larger than the limit of %d bytes", maxBytes)
		}
		return nil, 0, err
	}
	return file, size, nil
}

// parseContentRange reads a header of the form "bytes 100-199/1234".
func parseContentRange(header string) (total, start int64, err error) {
	fields := strings.Fields(header)
	if len(fields) != 2 || fields[0] != "bytes" {
		return 0, 0, fmt.Errorf("%w: Content-Range is %q", errNoRange, header)
	}
	span, size, ok := strings.Cut(fields[1], "/")
	if !ok {
		return 0, 0, fmt.Errorf("%w: Content-Range is %q", errNoRange, header)
	}
	total, err = strconv.ParseInt(size, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: Content-Range is %q", errNoRange, header)
	}
	first, _, ok := strings.Cut(span, "-")
	if !ok {
		return 0, 0, fmt.Errorf("%w: Content-Range is %q", errNoRange, header)
	}
	start, err = strconv.ParseInt(first, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: Content-Range is %q", errNoRange, header)
	}
	return total, start, nil
}

// rangeReaderAt reads a remote file with HTTP range requests.
//
// It keeps one block, because archive/zip reads the central directory in many
// small steps and each one would otherwise cost a request.
type rangeReaderAt struct {
	ctx     context.Context
	client  *http.Client
	url     string
	headers map[string]string
	size    int64
	read    *atomic.Int64

	cacheStart int64
	cache      []byte
}

func (r *rangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= r.size {
		return 0, io.EOF
	}
	filled := 0
	for filled < len(p) {
		position := off + int64(filled)
		if position >= r.size {
			return filled, io.EOF
		}
		if !r.cached(position) {
			if err := r.fetch(position); err != nil {
				return filled, err
			}
		}
		copied := copy(p[filled:], r.cache[position-r.cacheStart:])
		filled += copied
	}
	return filled, nil
}

func (r *rangeReaderAt) cached(position int64) bool {
	return position >= r.cacheStart && position < r.cacheStart+int64(len(r.cache))
}

// fetch loads the block that holds the position.
func (r *rangeReaderAt) fetch(position int64) error {
	start := position / blockSize * blockSize
	end := start + blockSize - 1
	if end >= r.size {
		end = r.size - 1
	}

	request, err := http.NewRequestWithContext(r.ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return fmt.Errorf("gtfsstatic: cannot build a request for %s: %w", r.url, err)
	}
	for name, value := range r.headers {
		request.Header.Set(name, value)
	}
	request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	response, err := r.client.Do(request)
	if err != nil {
		return fmt.Errorf("gtfsstatic: cannot fetch %s: %w", r.url, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("%w: %s returned HTTP %d for a range request", errNoRange, r.url, response.StatusCode)
	}
	block, err := io.ReadAll(io.LimitReader(response.Body, blockSize))
	if err != nil {
		return fmt.Errorf("gtfsstatic: cannot read %s: %w", r.url, err)
	}
	r.read.Add(int64(len(block)))
	r.cacheStart = start
	r.cache = block
	return nil
}
