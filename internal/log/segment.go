package log

import (
	"errors"
	"fmt"
	api "github.com/a-shakra/commit-log/api/v1"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
)

var (
	defaultIndexSizeBytes uint64 = 1024
	defaultStoreSizeBytes        = defaultIndexSizeBytes * 15
)

type segment struct {
	store             *store
	index             *index
	baseOffset        uint64
	nextOffset        uint64
	maxIndexSizeBytes uint64
	maxStoreSizeBytes uint64
	isFull            bool
}

func newSegment(dir string, baseOffset uint64, opts *segmentOptions) (*segment, error) {

	var iSize uint64
	if opts.maxIndexSizeBytes == nil {
		iSize = defaultIndexSizeBytes
	} else {
		iSize = *opts.maxIndexSizeBytes
	}

	var sSize uint64
	if opts.maxStoreSizeBytes == nil {
		sSize = defaultStoreSizeBytes
	} else {
		sSize = *opts.maxStoreSizeBytes
	}

	s := &segment{
		baseOffset:        baseOffset,
		maxIndexSizeBytes: iSize,
		maxStoreSizeBytes: sSize,
	}

	// initialize store
	sFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, // O_APPEND sets the file pointer to end of file to facilitate append operation
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.store, err = newStore(sFile, s.maxStoreSizeBytes)
	if err != nil {
		return nil, err
	}

	// initialize index
	iFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.index, err = newIndex(iFile, s.maxIndexSizeBytes)
	if err != nil {
		return nil, err
	}

	// get last offset if existing file, otherwise next offset is the base offset
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = s.baseOffset
	} else {
		s.nextOffset = s.baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append receives a record as input and stores that record into the index and store of the segment object
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	pRec, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(pRec)
	if err != nil {
		if errors.Is(err, ErrFileFull) {
			s.isFull = true
		}
		return 0, err
	}

	if err = s.index.Write(
		uint32(s.nextOffset-s.baseOffset), // converting absolute offset to relative offset for index entry
		pos,
	); err != nil {
		if errors.Is(err, ErrFileFull) {
			s.isFull = true
		}
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

// Read takes the absolute offset of the record as input and returns the record in the store
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	pRec, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	var record api.Record
	err = proto.Unmarshal(pRec, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// Close closes the open resources consumed by the index and store objects of the segment
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// Remove calls the close function of the segment object and then removes the files used to store the index and store
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// IsFull indicates whether the capacity of the segment has been exceeded
func (s *segment) IsFull() bool {
	return s.isFull
}
