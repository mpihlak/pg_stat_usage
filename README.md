Introduction
============
pg\_stat\_usage is a custom statistics collector for PostgreSQL. It intercepts
the stats that PostgreSQL backends send to the statistics collector by hooking 
into pgstat\_report\_stats calls.

Requires PostgreSQL stats collector hooks patch - included in the patches directory.

Building
========
USE\_PGXS=1 make install

Usage
=====
Modify postgresql.conf to include:

```
shared_preload_libraries = 'pg_stat_usage'
```
