package log

import (
	"errors"
	"fmt"
	"github.com/tysonmote/gommap"
	"os"
)

var (
	entryOffsetBytes    uint64 = 4
	recordStorePosBytes uint64 = 8
	totalEntrySizeBytes        = entryOffsetBytes + recordStorePosBytes
)

type index struct {
	file         *os.File
	mmap         gommap.MMap
	size         uint64
	maxSizeBytes uint64
}

func newIndex(f *os.File, maxSize uint64) (*index, error) {
	if maxSize == 0 {
		return nil, errors.New("index max size should be a non-zero value")
	}

	idx := &index{
		file:         f,
		maxSizeBytes: maxSize,
	}

	fInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fInfo.Size())
	if err = os.Truncate(f.Name(), int64(idx.maxSizeBytes)); err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

// Name returns the name of the file that contains the index's entries
func (i *index) Name() string {
	return i.file.Name()
}

// Read takes an offset input that points to a record entry in the index.
// Returns the position of the index entry if input is a positive value,
// otherwise returns the position of the last index entry, along with the
// position of the record in the store
func (i *index) Read(in int64) (offset uint32, position uint64, err error) {
	if i.size == 0 {
		return 0, 0, ErrEndOfFile
	}
	if in == -1 {
		offset = uint32((i.size / totalEntrySizeBytes) - 1)
	} else {
		offset = uint32(in)
	}
	position = uint64(offset) * totalEntrySizeBytes
	if i.size < position+totalEntrySizeBytes {
		return 0, 0, ErrEndOfFile
	}
	offset = encoding.Uint32(i.mmap[position : position+entryOffsetBytes])
	position = encoding.Uint64(i.mmap[position+entryOffsetBytes : position+totalEntrySizeBytes])
	return offset, position, nil
}

// Write takes an offset that is used to store the record's index entry
// along with the position of the record in the store that is associated
// with this index.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+totalEntrySizeBytes {
		return fmt.Errorf("index: %w", ErrFileFull)
	}
	encoding.PutUint32(i.mmap[i.size:i.size+entryOffsetBytes], off)
	encoding.PutUint64(i.mmap[i.size+entryOffsetBytes:i.size+totalEntrySizeBytes], pos)
	i.size += totalEntrySizeBytes
	return nil
}

// Close initiates a graceful shutdown of the index by adjusting
// file size to include actual file contents and not the maximum
// segment size that was originally configured for memory mapping
// purposes. A similar adjustment is made to the memory mapping
// structure.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}
