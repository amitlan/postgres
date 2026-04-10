/*-------------------------------------------------------------------------
 *
 * test_spi_resowner.c
 *		SQL-callable C function that uses SPI to execute a query.
 *
 *		Useful for testing code paths that only trigger under C-level
 *		SPI (not PL/pgSQL), such as resource owner interactions with
 *		RI fast-path FK checks.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_spi_resowner/test_spi_resowner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/spi.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(spi_exec_sql);

/*
 * spi_exec_sql(query text) - execute a SQL query via SPI.
 *
 * Opens a fresh SPI connection, executes the query, and closes the
 * connection.  Creates a dedicated child resource owner around the
 * SPI_execute call and releases it before returning, ensuring that
 * any resources registered under it (such as relation references
 * opened by RI fast-path FK checks) are released before the outer
 * trigger-firing batch callback fires.  This reproduces the resource
 * owner mismatch that occurs with C-language extensions like PostGIS
 * topology functions, which cannot be triggered from PL/pgSQL since
 * PL/pgSQL's SPI connection spans the entire function call.
 */
Datum
spi_exec_sql(PG_FUNCTION_ARGS)
{
	const char *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int			ret;
	ResourceOwner save = CurrentResourceOwner;
	ResourceOwner childowner = ResourceOwnerCreate(save, "test_spi inner");

	SPI_connect();

	CurrentResourceOwner = childowner;
	ret = SPI_execute(query, false, 0);

	if (ret < 0)
		elog(ERROR, "SPI_execute failed: error code %d", ret);

	SPI_finish();

	CurrentResourceOwner = save;
	ResourceOwnerRelease(childowner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 true, false);
	ResourceOwnerRelease(childowner,
						 RESOURCE_RELEASE_LOCKS,
						 true, false);
	ResourceOwnerRelease(childowner,
						 RESOURCE_RELEASE_AFTER_LOCKS,
						 true, false);
	ResourceOwnerDelete(childowner);

	PG_RETURN_VOID();
}
