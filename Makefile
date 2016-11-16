MODULE_big = pg_stat_usage
OBJS = pg_stat_usage.o

SHLIB_LINK = $(filter, $(LIBS))

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
