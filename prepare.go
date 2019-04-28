package db

import (
	"net/url"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"upper.io/db.v3/mongo"
	"upper.io/db.v3/mssql"
	"upper.io/db.v3/mysql"
	"upper.io/db.v3/postgresql"
)

const (
	databaseKey = "database"

	mongoScheme   = "mongodb"
	mysqlScheme   = "mysql://"
	postgreScheme = "postgres"
)

var replacements = map[string]string{
	"parsetime": "parseTime",
}

func prepareConfig(key string, v *viper.Viper, log *zap.Logger) ([]ConnectionOption, error) {
	if key == "" || !v.IsSet(key+".adapter") {
		return nil, errors.Wrapf(ErrConfigNotFound, key)
	}

	var (
		adapter = v.GetString(key + ".adapter")
		opts    = make([]ConnectionOption, 0, 4)
		vals    = make(url.Values)
		conf    = &url.URL{
			Scheme: adapter,
			Host:   v.GetString(key + ".hostname"),
			Path:   v.GetString(key + ".database"),
		}
	)

	if adapter == "" {
		return nil, ErrEmptyAdapter
	}

	conf.Host = v.GetString(key + ".hostname")

	switch adapter {
	case postgreScheme, postgresql.Adapter:
		adapter = postgresql.Adapter
		conf.Scheme = postgreScheme

	case mysql.Adapter:
		adapter = mysql.Adapter
		conf.Scheme = mysql.Adapter
		conf.Host = "tcp(" + conf.Host + ")"

	case mssql.Adapter:
		adapter = mssql.Adapter
		conf.Scheme = mssql.Adapter

	case mongoScheme, mongo.Adapter:
		adapter = mongo.Adapter
		conf.Scheme = mongoScheme
	default:
		return nil, ErrUnknownAdapter
	}

	conf.User = url.UserPassword(
		v.GetString(key+".username"),
		v.GetString(key+".password"))

	if options := v.GetStringMap(key + ".options"); len(options) > 0 {
		for opt := range options {
			if rep, ok := replacements[opt]; ok {
				opt = rep
			}
			vals[opt] = v.GetStringSlice(key + ".options." + opt)
		}
		conf.RawQuery = vals.Encode()
	}

	opts = append(opts,
		Adapter(adapter),
		Config(urlWrapper{
			url:     conf,
			adapter: adapter,
		}))

	if v.GetBool(key + ".debug") {
		opts = append(opts, Debug())
	}

	if log != nil {
		opts = append(opts, Logger(log))

		log.Debug("connection options",
			zap.String("key", key),
			zap.String("adapter", "mysql"),
			zap.Any("options", conf.String()))
	}

	return opts, nil
}
