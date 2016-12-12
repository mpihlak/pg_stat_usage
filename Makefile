MODULE_big = pg_stat_usage
OBJS = pg_stat_usage.o

EXTENSION = pg_stat_usage
DATA = pg_stat_usage--1.0.sql
PGFILEDESC = "pg_stat_usage - usage stats of stored procedure calls"

SHLIB_LINK = $(filter, $(LIBS))

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
