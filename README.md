# redis-cdc-crud-loader

## Download

Download the [latest release](https://github.com/RedisLabs-Field-Engineering/redis-cdc-crud-loader/releases/download/v1.0/redis-cdc-crud-loader-1.0-SNAPSHOT.tar.gz) and untar (tar -xvf redis-cdc-crud-loader-1.0-SNAPSHOT.tar.gz) the redis-cdc-crud-loader-1.0-SNAPSHOT.tar.gz archive.

All the contents would be extracted under redis-cdc-integration-test directory

Contents of redis-cdc-crud-loader
<br>•	bin – contains script files
<br>•	lib – contains java libraries
<br>•	config – contains sample config and data files for crud loader

## Launch

<br>*nix
`redis-cdc-crud-loader/bin$ ./start.sh`
<br>Windows
`redis-cdc-crud-loader\bin> start.bat`

```bash
Usage: redis-cdc-crud-loader [OPTIONS] [COMMAND]
CRUD loader for redis-cdc with random Insert, Update and Delete events.
  -h, --help   Show this help message and exit.
Commands:
  crudloader  Load CSV data to source database and execute random Insert, Update and Delete events.
  loadsql     Load data into source table using sql insert statements.
```
