package db

import (
	"net/url"
	"strings"

	"go.uber.org/dig"
	"go.uber.org/zap"
	"upper.io/db.v3"
)

type (
	// MySQL is wrapper over upper/db Database
	MySQL interface {
		db.Database
	}

	// PG is wrapper over upper/db Database
	PG interface {
		db.Database
	}

	// ConnectionResult for default module
	ConnectionResult struct {
		dig.Out

		MySQL MySQL `optional:"true" name:"default_mysql"`
		PG    PG    `optional:"true" name:"default_pg"`
	}

	// ConnectionOptions will be passed into NewConnection
	ConnectionOptions struct {
		Logger  *zap.Logger
		URL     db.ConnectionURL
		Debug   bool
		Adapter string
	}

	// ConnectionOption will be passed into NewConnection
	ConnectionOption func(opts *ConnectionOptions)

	// Error is constant errors
	Error string

	urlWrapper struct {
		url     *url.URL
		adapter string
	}

	logger struct {
		*zap.Logger
	}
)

const (
	// ErrEmptyConfig throws when db.ConnectionURL not passed into NewConnection
	ErrEmptyConfig = Error("empty config")

	// ErrEmptyAdapter throws when passed empty adapter into NewConnection
	ErrEmptyAdapter = Error("empty adapter")
	// ErrUnknownAdapter throws when passed unknown adapter into NewConnection
	ErrUnknownAdapter = Error("unknown adapter")

	// ErrConfigNotFound throws when config key is empty or not set
	ErrConfigNotFound = Error("configuration for key not found")
)

var replacer = strings.NewReplacer("\n", "", "\t", " ", "      ", "", "  ", " ", "   ", " ")

func (e Error) Error() string {
	return string(e)
}

func (l logger) Log(state *db.QueryStatus) {
	l.Logger.Debug("exec query",
		zap.Stringer("spent", state.End.Sub(state.Start)),
		zap.String("query", replacer.Replace(state.Query)),
		zap.Any("args", state.Args))
}
