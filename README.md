# redis-cdc-crud-loader

## Prerequisites

<br>• RedisCDC connector (e.g. mssql) is setup and running with the same source database, schema and table(s).
<br>• Please have Java Runtime Environment ([OpenJRE](https://openjdk.java.net/install/) or OracleJRE) installed prior to running redis-cdc-crud-loader.

## Download

<br>Download the [latest release](https://github.com/RedisLabs-Field-Engineering/redis-cdc-crud-loader/releases/download/v1.0/redis-cdc-crud-loader-1.0-SNAPSHOT.tar.gz) and untar (tar -xvf redis-cdc-crud-loader-1.0-SNAPSHOT.tar.gz) the redis-cdc-crud-loader-1.0-SNAPSHOT.tar.gz archive.

All the contents would be extracted under redis-cdc-integration-test directory

Contents of redis-cdc-crud-loader
<br>•	bin – contains script files
<br>•	lib – contains java libraries
<br>•	config – contains sample config and data files for crud loader

## Configuration

<br>redis-cdc-crud-loader includes drivers for the most common RDBMSs:

<br>• Oracle
<br>jdbc:oracle:thin:@myhost:1521:orcl

<br>• IBM DB2
<br>jdbc:db2://host:port/database

<br>• MS SQL Server
<br>jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]

<br>• MySQL
<br>jdbc:mysql://[host]:[port][/database][?properties]

<br>• PostgreSQL
<br>jdbc:postgresql://host:port/database

<details><summary>Sample config.yml</summary>
<p>

#### Sample config.yml under redis-cdc-crud-loader/config folder
```yml
connections:
  source:
    username: sa #DB user
    password: Redis@123 #DB password
    type: mssqlserver #this value can not be changed for mssqlserver
    jdbcUrl: "jdbc:sqlserver://127.0.0.1:1433;database=RedisLabsCDC"
    maximumPoolSize: 10 #This property controls the maximum size that the pool is allowed to reach, including both idle and in-use connections.
    minimumIdle: 2 #This property controls the maximum amount of time that a connection is allowed to sit idle in the pool. This setting only applies when minimumIdle is defined to be less than maximumPoolSize.
    tableName: dbo.emp #table name in schema.table format
    batchSize: 100 #batch size
    loadQueryFile: insert.sql #insert query for loadsql option
    csvFile: emp.csv #csv data with header to load (crudloader option)
    select: select.sql #select query for the continuous crud (crudloader option)
    updatedSelect: updatedSelect.sql #updated select query for the continuous crud
    update: update.sql #update query for the continuous crud (crudloader option)
    delete: delete.sql #delete query for the continuous crud (crudloader option)
    #loadQuery: "select * from dbo.emp" #This can be used instead of loadQueryFile property
    iteration: 100 #number of iterations to run (crudloader option)
```

</p>
</details>

## Generate Data

<br>Sample [emp table schema](https://www.mockaroo.com/f1faabd0) as provided in the package.

Generate data using cURL with the following command:
<br>`curl "https://api.mockaroo.com/api/f1faabd0?count=1000&key=a4b4ee70" > "emp.csv"`

## Launch

<br>[*nix OS](https://en.wikipedia.org/wiki/Unix-like):
`redis-cdc-crud-loader/bin$ ./start.sh`
<br>Windows OS:
`redis-cdc-crud-loader\bin> start.bat`

```bash
Usage: redis-cdc-crud-loader [OPTIONS] [COMMAND]
CRUD loader for redis-cdc with random Insert, Update and Delete events.
  -h, --help   Show this help message and exit.
Commands:
  crudloader  Load CSV data to source database and execute random Insert, Update and Delete events.
  loadsql     Load data into source table using sql insert statements.
```
