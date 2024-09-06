package storage

import "context"

type Storage interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
	Close(ctx context.Context) error
}
