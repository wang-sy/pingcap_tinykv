package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/pingcap/errors"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if value == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}

	return &kvrpcpb.RawGetResponse{Value: value}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	modify := storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}

	if err := server.storage.Write(req.Context, []storage.Modify{modify}); err != nil {
		return nil, errors.WithStack(err)
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	modify := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}

	if err := server.storage.Write(req.Context, []storage.Modify{modify}); err != nil {
		return nil, errors.WithStack(err)
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	iter.Seek(req.StartKey)

	kvres := []*kvrpcpb.KvPair{}
	for iter.Valid() {
		value, err := iter.Item().Value()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		kvres = append(kvres, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: value,
		})

		req.Limit--
		if req.Limit == 0 {
			break
		}

		iter.Next()
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvres}, nil
}
