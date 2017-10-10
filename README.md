# SIT

_Stupid Import tool_



_This program is currently under heavy development.
It may be unusable for your usecase, incompatible with 
your SQL-Server instance or even be unusable at all_




## Goal

Provide a line oriented database import / export tool,
targeting _CSV_.

_CSV is simple and efficient when used outside
of the Microsoft ecosystem. Let's take it back._


Current supported databases:

- SQL-Server (pymssql)
- sqlite (sqlite3)
	

Maybe a pyodbc support will be implemented in a near future.
(_pymssql_ seems hard to setup on a few OSes)



## Usage


```help
usage: sit [-h] [-c CONFIG] [-u USER] [-e ENGINE] [-p PASSWORD] [-t HOSTNAME]
           [-d DATABASE] [--port PORT]
           {raw,push,pull} ... env

positional arguments:
  {raw,push,pull}       * sub-commands help *
    raw                 open an SQL prompt to the remote server.You'll need
                        `pygment` and `prompt_toolkit`.
    push                Copy data to remote server. Can take regular CSV
                        directly from STDIN.
    pull                Retrieve data from remote server. Outputs regular CSV
                        on stdout
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

### pull

_Retrieve data_

```
usage: sit pull [-h] [-f FILE] [-T [TABLE_NAME]] [-q [QUERY]]

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  write to file instead of stdout
  -T [TABLE_NAME], --table [TABLE_NAME]
                        extract data from this table
  -q [QUERY], --query [QUERY]
                        
                        run a custom sqlquery for data retrieval.
                        
                        example:
                        
                        $ sit pull -q 'SELECT *
                        > FROM users U
                        > ORDER BY U.points
                        > LIMIT 100' staging > top_100_users.csv

```

### push

_Insert data (also, pretty obvious)_


```
usage: sit push [-h] [-f FILE] [-T [TABLE_NAME]] [-C]

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  read from file instead of stdin
  -T [TABLE_NAME], --table [TABLE_NAME]
                        INSERT into this table (default to current timestamp)
  -C, --create          automatically attempt to create a new table.
                        Column types will be naively inferred from first
                        lines of data.
                        eg:
                        `$ sit push -f dummy.csv -CT dummy_insert dev`
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
# naming table after the current timestamp.
#
# CSV comes from HTTP. Also, using pipes, 
# no intermediate files are needed
$ curl https://someurl/data.csv | sit push -C dev
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
$ sit pull -T test lite | python sit push -CT test dev
```


## How to setup/install/run


### pip

```bash
$ git clone git@github.com:slashdevsda/sit.git && cd sit
$ pip install .
```

### devel

```bash
$ git clone git@github.com:slashdevsda/sit.git && cd sit
$ virtualenv venv && source venv/bin/activate
(venv)$ pip install -r requirements.txt
```

### tests

```bash
$ git clone git@github.com:slashdevsda/sit.git && cd sit
$ virtualenv venv && source venv/bin/activate
(venv)$ pip install .
(venv)$ pip install pytest
(venv)$ pytest tests
```


## Features


### RAW

Opens a shell on the remote (or local) server.
Essentially for debugging purpose (and delete faulty insertions).


Note: it uses _prompt_toolkit_ and does not fallback on anything yet
if the terminal does not supports fancy termcaps.

This feature may disappear.


### About push feature and data insertion


The _push_ feature is not suitable for:

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
meaning you could have to run some additional queries to tranform the data.

