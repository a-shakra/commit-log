package log

type segmentOptions struct {
	maxIndexSizeBytes *uint64
	maxStoreSizeBytes *uint64
	initialOffset     *uint64
}

type options struct {
	segmentOptions segmentOptions
}

type Options func(options *options) error

func WithSegmentParams(iSize uint64, sSize uint64, iOff uint64) Options {
	return func(options *options) error {
		options.segmentOptions.maxIndexSizeBytes = &iSize
		options.segmentOptions.maxStoreSizeBytes = &sSize
		options.segmentOptions.initialOffset = &iOff
		return nil
	}
}
