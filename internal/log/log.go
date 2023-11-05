package log

import (
	"fmt"
	api "github.com/a-shakra/commit-log/api/v1"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

// Log represents the entire write-ahead log store in the given directory.
// It maintains a references to all segments that contain data and has
// access to the current active segment that data will be written to
type Log struct {
	mu sync.RWMutex

	Dir           string
	activeSegment *segment
	segments      []*segment
	options       *options
}

// NewLog returns an instance of a Log object that contains
// no data if initialized in an empty dict or contains data
// if store and index files exist in the given directory
func NewLog(dir string, opts ...Options) (*Log, error) {
	var lOpts options
	for _, opt := range opts {
		err := opt(&lOpts)
		if err != nil {
			return nil, fmt.Errorf("error on log creation: %v", err)
		}
	}
	l := &Log{
		Dir:     dir,
		options: &lOpts,
	}

	if err := l.setup(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	for i, off := range baseOffsets {
		// record every other offset since segment obj exists in a pair of index and store files
		if i%2 == 0 {
			err = l.newSegment(off)
			if err != nil {
				return err
			}
		}
	}
	if l.segments == nil {
		if err = l.newSegment(*l.options.segmentOptions.initialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, &l.options.segmentOptions)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

// Append stores a record object into the next available offset in
// the current active segment
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		if l.activeSegment.IsFull() {
			err = l.newSegment(off + 1)
		}
	}
	return off, err
}

// Read returns the record that is stored in the log
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, seg := range l.segments {
		if seg.baseOffset <= off && off < seg.nextOffset {
			s = seg
			break
		}
	}

	if s == nil {
		return nil, fmt.Errorf("offset is out of range: %d", off)
	}

	rec, err := s.Read(off)
	return rec, err
}

// Close closes all consumed resources
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove closes all consumed resources and deletes all Log data files
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset closes all consumed resources, deletes all Log data files
// then restores the Log to a new empty state
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	err := os.MkdirAll(l.Dir, 0644)
	if err != nil {
		return err
	}

	err = l.setup()
	if err != nil {
		return err
	}
	return nil
}
