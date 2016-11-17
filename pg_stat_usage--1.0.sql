-- complain if script is sourced in psql, rather than via CREATE EXTENSION
-- \echo Use "CREATE EXTENSION pg_stat_statements" to load this file. \quit

DROP FUNCTION pg_stat_usage() CASCADE;

CREATE FUNCTION pg_stat_usage(
	OUT object_oid oid,
	OUT parent_oid oid,
	OUT object_type text,
    OUT object_schema text,
    OUT object_name text,
	OUT num_calls int8
)
RETURNS SETOF record
AS '$libdir/pg_stat_usage'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

--AS 'MODULE_PATHNAME', 'pg_stat_usage_1_0'
--LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW pg_stat_usage AS
  SELECT * FROM pg_stat_usage();

GRANT SELECT ON pg_stat_usage TO PUBLIC;

