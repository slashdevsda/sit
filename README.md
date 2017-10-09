# SIT

_Stupid Import tool_





## Goal

Provide a line oriented database importation / exportation tool,
targeting _CSV_.

_CSV is simple and efficient when used outside
of the Microsoft ecosystem. Let's take it back._


## Usage

```help
usage: sit [-h] [-c CONFIG] [-u USER] [-e ENGINE] [-p PASSWORD] [-t HOSTNAME]
              [-d DATABASE] [--port PORT]
              {edit,raw,push,pull} ... env

positional arguments:
  {edit,raw,push,pull}  * sub-commands help *
    edit                (not implemented) edit an arbitrary object,
	                    using $EDITOR variable. In
                        case of a table, only the first 100 rows will be
                        fetched.
    raw                 open an SQL shell on the remote server.
    push                Copy data to remote server. Can take regular CSV
                        directly from STDIN.
    pull                Retrieve data from remote server.
	                    Outputs regular CSV on stdout
  env                   environement

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        read configuration from file (default to ./sit.ini,
                        fallback on ~/.sit.ini (default: None)
  -u USER, --user USER  username used (default: None)
  -e ENGINE, --engine ENGINE
                        db engine/driver (default: None)
  -p PASSWORD, --password PASSWORD
                        password used (default: None)
  -t HOSTNAME, --hostname HOSTNAME
                        database hostname (IP are ok) (default: None)
  -d DATABASE, --database DATABASE
                        which database to connect to (default: None)
  --port PORT           service port (default: None)
```


## Ini file

When invoked, `sit` looks for an `sit.ini` file located in your current directory.
This file primarily avoids very long command lines.


```ini
[dev]
hostname = MSSQL-DV1-062
port = 2003
user = DOMAIN\\username
password = mypassword
database = test_database
driver = sqlserver

[local_sqlite]
database = sqlite_file.db
driver = sqlite
```



```shell
# Create a table containing each line of records.csv,
# (after unzip/untar)
# naming table after the current timestamp.
# using pipes, no intermediate files are needed
$ tar -f records.csv.tar.gz -zx - | sit push -C dev
```


```shell
# Create a table containing each line of records.csv,
# into a local sqlite database. the table will
# be named "records" (-T argument)
$ sit push -f records.csv -CT records local_sqlite
```

```shell
# Opens a shell on the remote SQL-Server instance:
$ sit raw dev
```


```shell
# Fetch "test"'s table content from env. `lite`, then
# insert it into the table named "test", creating it
# if it does not exists.
#
# since we are using pipes and python's generator,
# the one below should work even with big files
$ sit pull -T test lite | python sit push -CT test dev
```

## Features

### RAW

Opens a shell on the remote (or local) server.
Essentially for debugging purpose (and delete faulty insertions).


Note: it uses _prompt_toolkit_ and does not fallback on anything yet
if the terminal does not supports fancy termcaps.


### COPY

Tranfert data from a CSV file (_**Comma** Separated Values_) into
a database table, created (or not) on purposes.


The copy feature is not
suitable for:

- very larges datasets

Since there is only one transaction
when inserting lines from a csv file,
it is not suitable for very large datasets,
and not very fault tolerant over a bad
connection. However, this could be implemented,
depending on usage.


- speed

this tool aims to speedup development and
operational tasks. Tt is not sufficiently reliable for a more
automated usage (ie: you should not use it to performs automated backups,
at least for the moment)

- smart stuff

This feature is not smart, even when it tries to guess
columns types. Basically, it will fallback on VARCHAR/derivatives when stuck,
meaning you could have to run some manipulation to
fit your own type logic and usage.
