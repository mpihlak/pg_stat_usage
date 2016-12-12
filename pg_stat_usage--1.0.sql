-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_stat_usage" to load this file. \quit

CREATE FUNCTION pg_stat_usage(
	OUT object_oid oid,
	OUT parent_oid oid,
	OUT object_type text,
    OUT object_schema text,
    OUT object_name text,
	OUT num_calls int8,
	OUT num_scans int8,
	OUT total_time int8,
	OUT self_time int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE FUNCTION pg_stat_usage_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW pg_stat_usage AS
  SELECT * FROM pg_stat_usage();

GRANT SELECT ON pg_stat_usage TO PUBLIC;

