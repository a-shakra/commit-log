package log

import (
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

type SegmentTestSuite struct {
	suite.Suite
	testDir string
	seg     *segment
}

func TestSegmentTestSuite(t *testing.T) {
	suite.Run(t, &SegmentTestSuite{})
}

func (s *SegmentTestSuite) SetupTest() {
	dir, err := os.MkdirTemp("", "segment-test-dir")
	s.Require().NoError(err)
	s.testDir = dir
	seg, err := newSegment(dir,
		testBaseOffset,
		&segmentOptions{maxIndexSizeBytes: &testIndexSize, maxStoreSizeBytes: &testStoreSize},
	)
	s.seg = seg
	s.Require().NoError(err)
	s.Require().Equal(testBaseOffset, s.seg.nextOffset)
}

func (s *SegmentTestSuite) TearDownTest() {
	err := os.RemoveAll(s.testDir)
	s.Require().NoError(err)
}

func (s *SegmentTestSuite) TestAppendToSegment() {
	for i := 0; i < 2; i++ {
		off, err := s.seg.Append(testProtoRecord)
		s.Require().NoError(err)
		s.Require().Equal(testBaseOffset+uint64(i), off)

		ret, err := s.seg.Read(off)
		s.Require().NoError(err)
		s.Require().Equal(testProtoRecord.Value, ret.Value)
	}
}

func (s *SegmentTestSuite) TestAppendExceededIndexSize() {
	for i := 0; i < 2; i++ {
		off, err := s.seg.Append(testProtoRecord)
		s.Require().NoError(err)
		s.Require().Equal(testBaseOffset+uint64(i), off)
	}
	_, err := s.seg.Append(testProtoRecord)
	s.Require().Error(err)
	s.Require().ErrorIs(err, ErrFileFull)
	s.Require().Equal(true, s.seg.IsFull())
}

func (s *SegmentTestSuite) TestAppendExceededStoreSize() {
	// reinitializing the segment with specific index and store size to make sure store size exceeded first
	err := s.seg.Remove()
	s.Require().NoError(err)

	deflatedStoreSize := uint64(50)
	inflatedIndexSize := testIndexSize * 2
	newSeg, err := newSegment(
		s.testDir,
		testBaseOffset,
		&segmentOptions{maxIndexSizeBytes: &inflatedIndexSize, maxStoreSizeBytes: &deflatedStoreSize})
	s.seg = newSeg
	s.Require().NoError(err)
	// adding records to reach store maximum size
	for i := 0; i < 2; i++ {
		off, err := s.seg.Append(testProtoRecord)
		s.Require().NoError(err)
		s.Require().Equal(testBaseOffset+uint64(i), off)
	}
	// appending a record that is too large to fit in store to illicit an error response
	_, err = s.seg.Append(testProtoRecord)
	s.Require().Error(err)
	s.Require().ErrorIs(err, ErrFileFull)
	s.Require().Equal(true, s.seg.IsFull())
}
