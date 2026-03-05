/*-------------------------------------------------------------------------
 *
 * execRowBatch.c
 *		Helpers for RowBatch
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/execRowBatch.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/execRowBatch.h"

/*
 * RowBatchCreate
 *		Allocate and initialize a new RowBatch envelope.
 */
RowBatch *
RowBatchCreate(int max_rows)
{
	RowBatch   *b;

	Assert(max_rows > 0);

	b = palloc(sizeof(RowBatch));
	b->am_payload = NULL;
	b->ops = NULL;
	b->max_rows = max_rows;
	b->nrows = 0;
	b->pos = 0;
	b->materialized = false;
	b->slot = NULL;

	return b;
}

/*
 * RowBatchReset
 *		Reset an existing RowBatch envelope to empty.
 */
void
RowBatchReset(RowBatch *b, bool drop_slots)
{
	Assert(b != NULL);

	b->nrows = 0;
	b->pos = 0;
	b->materialized = false;
	/* b->slot belongs to the owning PlanState node */
}
