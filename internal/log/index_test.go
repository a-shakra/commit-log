package log

import (
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

type IndexTestSuite struct {
	suite.Suite
	index *index
}

func TestIndexTestSuite(t *testing.T) {
	suite.Run(t, &IndexTestSuite{})
}

func (s *IndexTestSuite) SetupTest() {
	f, err := os.CreateTemp("", "index_test_temp_file")
	s.Require().NoError(err)

	idx, err := newIndex(f, WithMaxIndexSize(1024))
	s.Require().NoError(err)
	s.Require().Equal(f.Name(), idx.Name())
	s.index = idx
}

func (s *IndexTestSuite) TearDownTest() {
	err := os.Remove(s.index.file.Name())
	s.Require().NoError(err)
}

func (s *IndexTestSuite) TestWriteToIndex() {
	s.appendToIndex(4)
}

func (s *IndexTestSuite) TestReadFromEmptyIndexThenFail() {
	var anyNb int64
	anyNb = 4
	_, _, err := s.index.Read(anyNb)
	s.Require().Equal(ErrEndOfFile, err)
}

func (s *IndexTestSuite) TestReadLastEntryFromIndex() {
	recordsAdded := 4
	s.appendToIndex(recordsAdded)
	off, _, err := s.index.Read(-1)
	s.Require().NoError(err)
	s.Require().Equal(uint32(recordsAdded-1), off)
}

func (s *IndexTestSuite) TestBuildIndexFromExistingFile() {
	s.appendToIndex(4)
	fName := s.index.file.Name()
	err := s.index.Close()
	s.Require().NoError(err)

	f, err := os.OpenFile(fName, os.O_RDWR, 0600)
	idx, err := newIndex(f, WithMaxIndexSize(1024))
	s.Require().NoError(err)
	s.index = idx

	_, _, err = s.index.Read(-1)
	s.Require().NoError(err)
}

// TODO TestReadEntryFromIndex should be updated with deterministic positions vals when appendToIndex supports this
func (s *IndexTestSuite) TestReadEntryFromIndex() {
	s.appendToIndex(4)
	_, pos, err := s.index.Read(0)
	s.Require().NoError(err)
	s.Require().Equal(uint64(0), pos)
	_, pos, err = s.index.Read(1)
	s.Require().NoError(err)
	s.Require().Equal(uint64(10), pos)
}

// TODO appendToIndex should assign position in configurable way to prevent tight coupling with test functions
func (s *IndexTestSuite) appendToIndex(recordsToAppend int) {
	for offset := 0; offset < recordsToAppend; offset++ {
		err := s.index.Write(uint32(offset), uint64(offset*10))
		s.Require().NoError(err)
	}
}
