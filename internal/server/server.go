package server

import (
	"context"
	api "github.com/a-shakra/commit-log/api/v1"
	"github.com/a-shakra/commit-log/internal/log"
)

// guarantee *grpc.server meets LogServer interface at compile time
var _ api.LogServer = (*grpcServer)(nil)

// grpcServer TODO improve error handling logic in the method functions
type grpcServer struct {
	api.UnimplementedLogServer
	log *log.Log
}

func newgrpcServer(log *log.Log) (s *grpcServer, err error) {
	s = &grpcServer{
		log: log,
	}
	return s, nil
}

// Produce sends a *api.ProduceRequest object to the Log object with a record that is to be stored.
// the offset at which this record has been stored is returned.
// Produce TODO implement retry logic if the err returned is because the active segment was full
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {

	offset, err := s.log.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume will return the record that is stored at the indicated offset by the *api.ConsumeRequest req object.
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
	rec, err := s.log.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: rec}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return nil
		}
	}
}

func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			if err != nil {
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
