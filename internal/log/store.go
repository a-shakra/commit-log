package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

var (
	encoding = binary.BigEndian
)

const recordLenMetadataBytes = 8

type store struct {
	file         *os.File
	mu           sync.Mutex
	buf          *bufio.Writer
	size         uint64
	maxSizeBytes uint64
}

func newStore(f *os.File, maxSize uint64) (*store, error) {
	if maxSize == 0 {
		return nil, errors.New("store max size should be a non-zero value")
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		file:         f,
		size:         size,
		buf:          bufio.NewWriter(f),
		maxSizeBytes: maxSize,
	}, nil
}

func (s *store) Name() string {
	return s.file.Name()
}

// Append returns three parameters.
// The first return is bytes of record written to log + prefix that is the size of the record in bytes.
// The second return is the pos in the store that this record can be found in.
// The third return is a potential error value.
func (s *store) Append(p []byte) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.size+uint64(len(p)) > s.maxSizeBytes {
		return 0, 0, fmt.Errorf("store: %w", ErrFileFull)
	}

	if err := binary.Write(s.buf, encoding, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	bytesWritten, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	bytesWritten += recordLenMetadataBytes
	recordOffset := s.size
	s.size += uint64(bytesWritten)

	return uint64(bytesWritten), recordOffset, nil
}

// Read returns the record in the store at the given position indicated by the recordOffset parameter
func (s *store) Read(position uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, recordLenMetadataBytes)
	if _, err := s.file.ReadAt(size, int64(position)); err != nil {
		return nil, ErrEndOfFile
	}

	res := make([]byte, encoding.Uint64(size))
	if _, err := s.file.ReadAt(res, int64(position+recordLenMetadataBytes)); err != nil {
		return nil, ErrEndOfFile
	}

	return res, nil
}

// ReadAt returns len(p) bytes from the store at the indicated position to the []byte input
func (s *store) ReadAt(p []byte, position int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	n, err := s.file.ReadAt(p, position)
	if err != nil {
		return 0, ErrEndOfFile
	}

	return n, nil
}

// Close makes sure that the buffer has flushed data to file before closing the file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.file.Close()
}
