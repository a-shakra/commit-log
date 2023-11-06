package server

import (
	"context"
	api "github.com/a-shakra/commit-log/api/v1"
	"github.com/a-shakra/commit-log/internal/log"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"testing"
)

type serverOpenResources struct {
	wal      WriteAheadLog
	client   api.LogClient
	server   *grpc.Server
	ccon     *grpc.ClientConn
	listener net.Listener
}

type ServerTestSuite struct {
	suite.Suite
	resources serverOpenResources
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, &ServerTestSuite{})
}

func (s *ServerTestSuite) SetupTest() {
	// :0 -> means the port is automatically chosen
	listener, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)

	s.Require().NoError(err)

	dir, err := os.MkdirTemp("", "server-test")
	s.Require().NoError(err)

	wal, err := log.NewLog(dir)
	s.Require().NoError(err)

	server, err := NewGrpcServer(wal)
	s.Require().NoError(err)

	go func() {
		server.Serve(listener)
	}()

	cOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	cconn, err := grpc.Dial(listener.Addr().String(), cOpts...)
	client := api.NewLogClient(cconn)

	resources := serverOpenResources{
		wal:      wal,
		client:   client,
		server:   server,
		ccon:     cconn,
		listener: listener,
	}
	s.resources = resources
}

func (s *ServerTestSuite) TearDownTest() {
	err := s.resources.wal.Remove()
	s.Require().NoError(err)
	err = s.resources.ccon.Close()
	s.Require().NoError(err)
	err = s.resources.listener.Close()
	s.Require().NoError(err)
	s.resources.server.Stop()
}

func (s *ServerTestSuite) TestProduceConsume() {
	ctx := context.Background()
	want := &api.Record{Value: []byte("test api record")}
	produce, err := s.resources.client.Produce(
		ctx,
		&api.ProduceRequest{Record: want},
	)
	s.Require().NoError(err)

	consume, err := s.resources.client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	s.Require().NoError(err)
	s.Require().Equal(want.Value, consume.Record.Value)
	s.Require().Equal(want.Offset, consume.Record.Offset)
}

func (s *ServerTestSuite) TestOffsetOutOfBounds() {
	ctx := context.Background()
	want := &api.Record{Value: []byte("test api record")}
	produce, err := s.resources.client.Produce(
		ctx,
		&api.ProduceRequest{Record: want},
	)
	s.Require().NoError(err)

	_, err = s.resources.client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 5})
	s.Require().Error(err)
	ret := status.Code(err)
	expected := status.Code(log.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	s.Require().Equal(expected, ret)
}

func (s *ServerTestSuite) TestProduceConsumeStream() {
	ctx := context.Background()
	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	{
		stream, err := s.resources.client.ProduceStream(ctx)
		s.Require().NoError(err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			s.Require().NoError(err)
			res, err := stream.Recv()
			s.Require().NoError(err)
			s.Require().Equal(uint64(offset), res.Offset)
		}
	}
	{
		stream, err := s.resources.client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		s.Require().NoError(err)
		for i, record := range records {
			res, err := stream.Recv()
			s.Require().NoError(err)
			s.Require().Equal(res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i)},
			)
		}
	}

}
