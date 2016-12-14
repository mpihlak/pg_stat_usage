# Introduction
pg\_stat\_usage is a custom statistics collector for PostgreSQL. It intercepts
the stats that PostgreSQL backends send to the statistics collector by hooking 
into pgstat\_report\_stats calls.

Presently stored procedure and relation (tables, indexes, etc.) stats are
collected.  When possible the context of a stored procedure call is also
stored. This can be used to generate a call graph of stored procedures and also
get some insight as to what goes on inside the stored procs (table, index
access etc.)

Requires PostgreSQL stats collector hooks patch - included in the patches directory.

## Ideas
 * Use CPU time instead of wall clock time
 * Figure out a way to reliably associate table stats with function context
 * Track role names and IP addresses
 * Add a collector process that aggregates the stats from different backends

## Building
Set up PostgreSQL module development environment and run `make install`. pg\_config needs to in the PATH.

## Usage
In an interactive session
```
LOAD 'pg_stat_usage';
...
-- run a function
select * from ff3();

-- get the stats
select * from pg_stat_usage();

 t | schema |  object_name  | object_oid | parent_oid | num_calls | num_scans
---+--------+---------------+------------+------------+-----------+-----------
 r | public | tt1           |      17447 |      16466 |         0 |         1
 F | public | pg_stat_usage |      16760 |          0 |         1 |         0
 i | public | tt1_pkey      |      17453 |      16466 |         0 |         0
 F | public | ff3           |      16466 |          0 |         1 |         0
(4 rows)
```

To enable pg\_stat\_usage for the entire cluster, modify postgresql.conf to include:

```
local_preload_libraries = 'pg_stat_usage'
```
