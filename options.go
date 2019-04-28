package db

import (
	"go.uber.org/zap"
	"upper.io/db.v3"
)

// Config pass connection url into options
func Config(cfg db.ConnectionURL) ConnectionOption {
	return func(opts *ConnectionOptions) {
		opts.URL = cfg
	}
}

// Adapter pass connection adapter into options
func Adapter(adapter string) ConnectionOption {
	return func(opts *ConnectionOptions) {
		opts.Adapter = adapter
	}
}

// Logger pass connection logger into options
func Logger(log *zap.Logger) ConnectionOption {
	return func(opts *ConnectionOptions) {
		opts.Logger = log
	}
}

// Debug enable connection debug
func Debug() ConnectionOption {
	return func(opts *ConnectionOptions) {
		opts.Debug = true
	}
}
