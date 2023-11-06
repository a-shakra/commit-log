package log

import (
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

type LogTestSuite struct {
	suite.Suite
	testDir string
	log     *Log
}

func TestLogTestSuite(t *testing.T) {
	suite.Run(t, &LogTestSuite{})
}

func (s *LogTestSuite) SetupTest() {
	dir, err := os.MkdirTemp("", "log-test")
	s.Require().NoError(err)
	s.testDir = dir

	log, err := NewLog(s.testDir, WithSegmentParams(testIndexSize, testStoreSize, testInitialOffset))
	s.Require().NoError(err)
	s.log = log
}

func (s *LogTestSuite) TearDownTest() {
	err := os.RemoveAll(s.testDir)
	s.Require().NoError(err)
}

func (s *LogTestSuite) TestAppendAndRead() {
	off, err := s.log.Append(testProtoRecord)
	s.Require().NoError(err)
	ret, err := s.log.Read(off)
	s.Require().NoError(err)
	s.Require().Equal(testProtoRecord.Value, ret.Value)
}

func (s *LogTestSuite) TestReadOutOfRange() {
	off, err := s.log.Append(testProtoRecord)
	s.Require().NoError(err)
	outOfRangeOff := off + 1
	_, err = s.log.Read(outOfRangeOff)
	s.Require().ErrorIs(err, ErrOffsetOutOfRange{Offset: outOfRangeOff})
}

func (s *LogTestSuite) TestInitClean() {
	s.Require().Equal(1, len(s.log.segments))
	s.Require().NotNil(s.log.activeSegment)
}

func (s *LogTestSuite) TestInitExistingFiles() {
	err := s.log.newSegment(0)
	s.Require().NoError(err)
	err = s.log.newSegment(100)
	s.Require().NoError(err)
	err = s.log.Close()
	s.Require().NoError(err)
	s.log, err = NewLog(s.testDir)
	s.Require().NoError(err)
	s.Require().Equal(2, len(s.log.segments))
}

func (s *LogTestSuite) TestClose() {
	err := s.log.Close()
	s.Require().NoError(err)
}

func (s *LogTestSuite) TestRemove() {
	err := s.log.Remove()
	s.Require().NoError(err)
	s.Require().NoDirExists(s.testDir)
}

func (s *LogTestSuite) TestReset() {
	err := s.log.Reset()
	s.Require().NoError(err)
	s.Require().DirExists(s.testDir)
}
