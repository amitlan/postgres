--
-- Test RI fast-path FK check under C-level SPI.
--
-- The RI fast-path caches PK relation references in ri_FastPathGetEntry()
-- under the current resource owner.  When FK triggers fire inside a
-- C-level SPI context that creates a dedicated short-lived resource owner,
-- those references must be released before the inner resource owner is
-- released.  The fix ensures batch callbacks fire at the same firing depth
-- at which they were registered, while the corresponding resource owner
-- is still alive.  Without this, ri_FastPathTeardown would crash with
-- Assert(rel->rd_refcnt > 0) in index_close.
--
-- Simple PL/pgSQL does not trigger this because its SPI connection spans
-- the entire function call, so its resource owner outlives the batch
-- callback.  The critical test case requires a C function that creates a
-- dedicated short-lived resource owner around its SPI call.
--
CREATE EXTENSION test_spi_resowner;

CREATE TABLE ri_fp_pk1 (id serial PRIMARY KEY);
CREATE TABLE ri_fp_pk2 (id serial PRIMARY KEY);
CREATE TABLE ri_fp_pk3 (id serial PRIMARY KEY);
INSERT INTO ri_fp_pk1 VALUES (1);
INSERT INTO ri_fp_pk2 VALUES (1);
INSERT INTO ri_fp_pk3 VALUES (1);

CREATE TABLE ri_fp_fk (
    id serial PRIMARY KEY,
    a int REFERENCES ri_fp_pk1(id),
    b int REFERENCES ri_fp_pk2(id),
    c int REFERENCES ri_fp_pk3(id),
    d int REFERENCES ri_fp_pk1(id),
    e int REFERENCES ri_fp_pk2(id),
    f int REFERENCES ri_fp_pk3(id)
);

-- C-level SPI INSERT: the critical test case.
SELECT spi_exec_sql(
    'INSERT INTO ri_fp_fk (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, 1)');

-- Additional C-level SPI INSERTs to exercise batch reuse across calls.
-- Use different column orderings to ensure each is a distinct statement.
SELECT spi_exec_sql(
    'INSERT INTO ri_fp_fk (f, e, d, c, b, a) VALUES (1, 1, 1, 1, 1, 1)');
SELECT spi_exec_sql(
    'INSERT INTO ri_fp_fk (a, c, e, b, d, f) VALUES (1, 1, 1, 1, 1, 1)');

-- C-level SPI with FK violation: should error
SELECT spi_exec_sql(
    'INSERT INTO ri_fp_fk (a, b, c, d, e, f) VALUES (999, 1, 1, 1, 1, 1)');

-- Nested: PL/pgSQL calling C SPI (mimics PostGIS toTopoGeom pattern)
CREATE FUNCTION plpgsql_calls_c_spi() RETURNS void AS $$
DECLARE
    ins_stmt text := 'INSERT INTO ri_fp_fk (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, 1)';
BEGIN
    PERFORM spi_exec_sql(ins_stmt);
END;
$$ LANGUAGE plpgsql;

SELECT plpgsql_calls_c_spi();

-- AFTER trigger that uses C-level SPI to insert into an FK-referencing table.
-- The FK batch callback is registered at the inner SPI's query level and
-- must fire before the inner resource owner is released.
CREATE TABLE ri_fp_outer (id int PRIMARY KEY);
CREATE TABLE ri_fp_inner (id int REFERENCES ri_fp_pk1(id));

CREATE FUNCTION outer_trigger_spi_ok() RETURNS trigger AS $$
BEGIN
    PERFORM spi_exec_sql('INSERT INTO ri_fp_inner VALUES (1)');
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER outer_tg AFTER INSERT ON ri_fp_outer
    FOR EACH ROW EXECUTE FUNCTION outer_trigger_spi_ok();

-- Fires outer_tg, whose PL/pgSQL body calls spi_exec_sql().  The C function
-- creates a dedicated resource owner that is released after the FK batch
-- callback fires.
INSERT INTO ri_fp_outer VALUES (1);

CREATE FUNCTION outer_trigger_spi_fail() RETURNS trigger AS $$
BEGIN
    PERFORM spi_exec_sql('INSERT INTO ri_fp_inner VALUES (3)');
    RETURN NEW;
END $$ LANGUAGE plpgsql;

DROP TRIGGER outer_tg ON ri_fp_outer;
DROP FUNCTION outer_trigger_spi_ok();

CREATE TRIGGER outer_tg AFTER INSERT ON ri_fp_outer
    FOR EACH ROW EXECUTE FUNCTION outer_trigger_spi_fail();

--  Like above but the inner insert fails.
INSERT INTO ri_fp_outer VALUES (2);

DROP TRIGGER outer_tg ON ri_fp_outer;
DROP FUNCTION outer_trigger_spi_fail();
DROP TABLE ri_fp_inner, ri_fp_outer;

-- Cleanup
DROP TABLE ri_fp_fk;
DROP TABLE ri_fp_pk3, ri_fp_pk2, ri_fp_pk1;
DROP EXTENSION test_spi_resowner;
