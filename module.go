package db

import (
	"github.com/im-kulikov/helium/module"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"upper.io/db.v3/lib/sqlbuilder"
)

var (
	// MySQLModule default connection to mysql
	MySQLModule = module.Module{
		{Constructor: newMySQLConnection},
	}

	// PostgresModule default connection to postgres
	PostgresModule = module.Module{
		{Constructor: newPostgresConnection},
	}
)

func newMySQLConnection(v *viper.Viper, l *zap.Logger) (MySQL, error) {
	opts, err := prepareConfig("database.mysql", v, l)
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare mysql config")
	}

	con, err := NewConnection(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "could not create connection")
	}

	builder := con.(sqlbuilder.SQLBuilder)

	return &mysqlConnection{
		Database:   con,
		SQLBuilder: builder,
	}, nil
}

func newPostgresConnection(v *viper.Viper, l *zap.Logger) (PG, error) {
	opts, err := prepareConfig("database.postgres", v, l)
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare postgres config")
	}

	con, err := NewConnection(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "could not create postgres connection")
	}

	builder := con.(sqlbuilder.SQLBuilder)

	return &postgresConnection{
		Database:   con,
		SQLBuilder: builder,
	}, nil
}
