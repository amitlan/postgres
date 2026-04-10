/* src/test/modules/test_spi_resowner/test_spi_resowner--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_spi_resowner" to load this file. \quit

CREATE FUNCTION spi_exec_sql(query text)
RETURNS void
AS 'MODULE_PATHNAME', 'spi_exec_sql'
LANGUAGE C STRICT;
