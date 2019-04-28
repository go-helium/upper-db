package db

import (
	"net/url"
	"strings"
	"testing"

	"github.com/im-kulikov/helium/module"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/dig"
	"go.uber.org/zap"
	"upper.io/db.v3"
)

type testAdapter struct {
	db.Database
}

const errTest = Error("test")

func (testAdapter) Ping() error { return errTest }

func testViper() (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	return v, v.ReadConfig(config)
}

func TestConnection(t *testing.T) {
	v, err := testViper()
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
		t.Run("by drivers", func(t *testing.T) {
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

		t.Run("by constructors", func(t *testing.T) {
			t.Run("mysql", func(t *testing.T) {
				con, err := newMySQLConnection(v, log)
				require.NoError(t, err)
				require.NoError(t, con.Close())
			})

			t.Run("postgres", func(t *testing.T) {
				con, err := newPostgresConnection(v, log)
				require.NoError(t, err)
				require.NoError(t, con.Close())
			})
		})

		t.Run("by modules", func(t *testing.T) {
			mod := module.Module{
				{Constructor: func() *zap.Logger { return log }},
				{Constructor: func() *viper.Viper { return v }},
			}.Append(
				MySQLModule,
				PostgresModule)

			di := dig.New()

			err := module.Provide(di, mod)
			require.NoError(t, err)

			t.Run("mysql", func(t *testing.T) {
				err := di.Invoke(func(con MySQL) {
					require.NoError(t, con.Close())
				})
				require.NoError(t, err)
			})

			t.Run("postgres", func(t *testing.T) {
				err := di.Invoke(func(con PG) {
					require.NoError(t, con.Close())
				})
				require.NoError(t, err)
			})
		})
	})

	t.Run("should fail", func(t *testing.T) {
		t.Run("by drivers", func(t *testing.T) {
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

		t.Run("by constructors", func(t *testing.T) {
			t.Run("mysql", func(t *testing.T) {
				t.Run("config", func(t *testing.T) {
					v, err := testViper()
					require.NoError(t, err)

					v.Set("database.mysql.adapter", "")
					_, err = newMySQLConnection(v, log)
					require.Error(t, err)
				})

				t.Run("connection", func(t *testing.T) {
					v, err := testViper()
					require.NoError(t, err)

					v.Set("database.mysql.adapter", "mysql")
					v.Set("database.mysql.hostname", "")
					_, err = newMySQLConnection(v, log)
					require.Error(t, err)
				})
			})

			t.Run("postgres", func(t *testing.T) {
				t.Run("config", func(t *testing.T) {
					v, err := testViper()
					require.NoError(t, err)

					v.Set("database.postgres.adapter", "")
					_, err = newPostgresConnection(v, log)
					require.Error(t, err)
				})

				t.Run("connection", func(t *testing.T) {
					v, err := testViper()
					require.NoError(t, err)

					v.Set("database.postgres.adapter", "postgres")
					v.Set("database.postgres.hostname", "")
					_, err = newPostgresConnection(v, log)
					require.Error(t, err)
				})
			})
		})
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
