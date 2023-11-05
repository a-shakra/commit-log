package log

import (
	api "github.com/a-shakra/commit-log/api/v1"
)

var (
	testRecord                 = []byte("test input")
	expectedWriteLength        = recordLenMetadataBytes + uint64(len(testRecord))
	maxStoreTestSize    uint64 = 1024
	maxIndexTestSize    uint64 = 1024
	testProtoRecord            = &api.Record{Value: []byte("test input")}
	testIndexSize       uint64 = 8 * 4
	testStoreSize              = testIndexSize * 4
	testBaseOffset      uint64 = 8 * 3
	testInitialOffset   uint64 = 0
)
