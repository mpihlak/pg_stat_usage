# Introduction
pg\_stat\_usage is a custom statistics collector for PostgreSQL. It intercepts
the stats that PostgreSQL backends send to the statistics collector by hooking 
into pgstat\_report\_stats calls.

Requires PostgreSQL stats collector hooks patch - included in the patches directory.

## Ideas
 * Collect relation stats per stored procedure
 * Collect RUSAGE times instead of elapsed times
 * Track the stats in shared memory (eg. like pg\_stat\_statements)
 * Track role names and IP addresses

## Building
Set up PostgreSQL module development environment and run
```
make install
```

## Usage
In an interactive session
```
LOAD pg_stat_usage
```

Alternatively modify postgresql.conf to include:

```
local_preload_libraries = 'pg_stat_usage'
```
