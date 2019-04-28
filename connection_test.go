package db

import (
	"net/url"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"upper.io/db.v3"
)

type testAdapter struct {
	db.Database
}

const errTest = Error("test")

func (testAdapter) Ping() error { return errTest }

func TestConnection(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")
	err := v.ReadConfig(config)
	require.NoError(t, err)

	// log := zap.L()

	// use for debug reason
	log, err := zap.NewDevelopment()
	require.NoError(t, err)

	t.Run("should fail with test adapter", func(t *testing.T) {
		db.RegisterAdapter("test", &db.AdapterFuncMap{
			Open: func(settings db.ConnectionURL) (db.Database, error) {
				return &testAdapter{}, nil
			},
		})

		_, err := NewConnection(
			Adapter("test"),
			Config(new(url.URL)))

		require.EqualError(t, errors.Cause(err), errTest.Error())
	})

	t.Run("should not fail", func(t *testing.T) {
		databases := []string{
			"database.mysql",
			"database.postgres",
		}

		for _, adapter := range databases {
			t.Run(adapter, func(t *testing.T) {
				conf, err := prepareConfig(adapter, v, log)
				require.NoError(t, err)
				conn, err := NewConnection(conf...)
				require.NoError(t, err)
				require.NoError(t, conn.Ping())
				_, err = conn.Collections()
				require.NoError(t, err)
				require.NoError(t, conn.Close())
			})
		}
	})

	t.Run("should fail", func(t *testing.T) {
		databases := []string{
			"database.mongo",
			"database.mssql",
		}

		for _, adapter := range databases {
			t.Run(adapter, func(t *testing.T) {
				conf, err := prepareConfig(adapter, v, log)
				require.NoError(t, err)
				_, err = NewConnection(conf...)
				require.Error(t, err)
			})
		}
	})

	t.Run("should fail ErrUnknownKey", func(t *testing.T) {
		_, err := prepareConfig(databaseKey+".unknown", v, log)
		require.EqualError(t,
			errors.Cause(err),
			ErrConfigNotFound.Error())
	})

	t.Run("should fail ErrUnknownAdapter", func(t *testing.T) {
		v.SetDefault("test.adapter", "test")
		_, err := prepareConfig("test", v, log)
		require.EqualError(t,
			errors.Cause(err),
			ErrUnknownAdapter.Error())
	})

	t.Run("should fail ErrEmptyAdapter", func(t *testing.T) {
		t.Run("for prepare config", func(t *testing.T) {
			v.SetDefault("test.adapter", "")
			_, err := prepareConfig("test", v, log)
			require.EqualError(t,
				errors.Cause(err),
				ErrEmptyAdapter.Error())
		})

		t.Run("for NewConnection", func(t *testing.T) {
			_, err := NewConnection()
			require.EqualError(t,
				errors.Cause(err),
				ErrEmptyAdapter.Error())
		})
	})

	t.Run("should fail ErrEmptyConfig", func(t *testing.T) {
		_, err := NewConnection(Adapter("test"))
		require.EqualError(t,
			errors.Cause(err),
			ErrEmptyConfig.Error())
	})

	t.Run("should fail on open connection", func(t *testing.T) {
		_, err := NewConnection(Adapter("test"))
		require.EqualError(t,
			errors.Cause(err),
			ErrEmptyConfig.Error())
	})
}

var config = strings.NewReader(`
database:
  mysql:
    adapter: mysql
    hostname: 127.0.0.1:3306
    database: mysql
    username: root
    password:
    debug: true
    options:
      parseTime: true
  postgres:
    adapter: postgres
    hostname: 127.0.0.1:5432
    database: postgres
    username: postgres
    password: postgres
    debug: true
    options:
      sslmode: disable
  mssql:
    adapter: mssql
    hostname: 127.0.0.1:1433
    database: master
    username: test
    password: test
    debug: true
  mongo:
    adapter: mongo
    hostname: 127.0.0.1:27017
    database: master
    username: test
    password: test
    debug: true
`)
