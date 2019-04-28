# upper.io/db.v3 Module for Helium

## Default modules

- MySQL - to connection `database.mysql`
- PG - to connection `database.postgres`

## Example configuration

```yaml
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
```