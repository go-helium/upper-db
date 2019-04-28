package db

import (
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"upper.io/db.v3"
	"upper.io/db.v3/mysql"
)

func wrapLogger(log *zap.Logger) db.Logger {
	return &logger{Logger: log}
}

func (u urlWrapper) String() string {
	res := u.url.String()

	if u.adapter == mysql.Adapter {
		res = strings.ReplaceAll(res, mysqlScheme, "")
	}

	return res
}

// NewConnection creates new database connection by adapter and config
//
// if cfg contains debug, than enable logging and replace default logger by zap.Logger
func NewConnection(opts ...ConnectionOption) (con db.Database, err error) {
	var cfg ConnectionOptions

	for _, opt := range opts {
		opt(&cfg)
	}

	switch {
	case cfg.Adapter == "":
		return nil, ErrEmptyAdapter
	case cfg.URL == nil:
		return nil, ErrEmptyConfig
	}

	if con, err = db.Open(cfg.Adapter, cfg.URL); err != nil {
		return nil, errors.Wrapf(err, "could not connect to `%s`", cfg.Adapter)
	}

	if err = con.Ping(); err != nil {
		return nil, errors.Wrapf(err, "could not ping `%s`", cfg.Adapter)
	}

	con.SetLogging(cfg.Debug)

	if cfg.Debug && cfg.Logger != nil {
		con.SetLogger(wrapLogger(cfg.Logger))
	}

	return con, nil
}
