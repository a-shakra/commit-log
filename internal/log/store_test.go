package log

import (
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

var (
	testRecord          = []byte("test input")
	expectedWriteLength = recordLenMetadataBytes + uint64(len(testRecord))
)

type StoreTestSuite struct {
	suite.Suite
	store *store
}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, &StoreTestSuite{})
}

func (s *StoreTestSuite) SetupTest() {
	f, err := os.CreateTemp("", "store_test_temp_file")
	s.Require().NoError(err)

	st, err := newStore(f)
	s.Require().NoError(err)
	s.store = st
}

func (s *StoreTestSuite) TearDownTest() {
	err := os.Remove(s.store.file.Name())
	s.Require().NoError(err)
}

func (s *StoreTestSuite) TestStoreAppend() {
	appendToStore(s, 4)
}

func (s *StoreTestSuite) TestStoreRead() {
	toAppend := 4
	appendToStore(s, toAppend)
	var pos uint64
	for i := 1; i < toAppend; i++ {
		record, err := s.store.Read(pos)
		s.Require().NoError(err)
		s.Require().Equal(testRecord, record)
		pos += expectedWriteLength
	}
}

func (s *StoreTestSuite) TestStoreReadEmptyThenFail() {
	record, err := s.store.Read(0)
	s.Require().Equal(err, ErrEndOfFile)
	s.Require().Empty(record)
}

func (s *StoreTestSuite) TestStoreReadAt() {
	toAppend := 4
	appendToStore(s, toAppend)

	for i, offset := 1, 0; i < toAppend; i++ {
		b := make([]byte, recordLenMetadataBytes)
		n, err := s.store.ReadAt(b, int64(offset))
		s.Require().NoError(err)
		s.Require().Equal(recordLenMetadataBytes, n)
		offset += n

		recordLength := encoding.Uint64(b)
		b = make([]byte, recordLength)
		n, err = s.store.ReadAt(b, int64(offset))
		s.Require().NoError(err)
		s.Require().Equal(testRecord, b)
		s.Require().Equal(int(recordLength), n)
		offset += n
	}
}

func (s *StoreTestSuite) TestStoreReadAtEmptyThenFail() {
	var testOffset int64
	b := make([]byte, recordLenMetadataBytes)
	_, err := s.store.ReadAt(b, testOffset)
	s.Require().Equal(err, ErrEndOfFile)
}

func (s *StoreTestSuite) TestStoreClose() {
	toAppend := 4
	appendToStore(s, toAppend)

	fInfo, err := os.Stat(s.store.file.Name())
	s.Require().NoError(err)
	oldfSize := fInfo.Size()

	err = s.store.Close()
	s.Require().NoError(err)

	fInfo, err = os.Stat(s.store.file.Name())
	newfSize := fInfo.Size()
	s.Require().True(newfSize > oldfSize)
}

func appendToStore(s *StoreTestSuite, recordsToAppend int) {
	for i := 1; i < recordsToAppend; i++ {
		n, pos, err := s.store.Append(testRecord)
		s.Require().NoError(err)
		s.Require().Equal(uint64(i)*expectedWriteLength, pos+n)
	}
}
