/* contrib/pg_plan_advice/pg_plan_advice--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_plan_advice" to load this file. \quit

CREATE FUNCTION pg_clear_collected_local_advice()
RETURNS void
AS 'MODULE_PATHNAME', 'pg_clear_collected_local_advice'
LANGUAGE C STRICT;

CREATE FUNCTION pg_clear_collected_shared_advice()
RETURNS void
AS 'MODULE_PATHNAME', 'pg_clear_collected_shared_advice'
LANGUAGE C STRICT;

CREATE FUNCTION pg_get_collected_local_advice(
	OUT id bigint,
	OUT userid oid,
	OUT dbid oid,
	OUT queryid bigint,
	OUT collection_time timestamptz,
	OUT query text,
	OUT advice text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_get_collected_local_advice'
LANGUAGE C STRICT;

CREATE FUNCTION pg_get_collected_shared_advice(
	OUT id bigint,
	OUT userid oid,
	OUT dbid oid,
	OUT queryid bigint,
	OUT collection_time timestamptz,
	OUT query text,
	OUT advice text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_get_collected_shared_advice'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION pg_get_collected_shared_advice() FROM PUBLIC;
