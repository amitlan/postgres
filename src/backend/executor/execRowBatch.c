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
RowBatchCreate(int max_rows, bool track_stats)
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

	if (track_stats)
	{
		RowBatchStats *stats = palloc_object(RowBatchStats);

		stats->batches = 0;
		stats->rows = 0;
		stats->max_rows = 0;
		stats->min_rows = INT_MAX;

		b->stats = stats;
	}
	else
		b->stats = NULL;

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

void
RowBatchRecordStats(RowBatch *b, int rows)
{
	RowBatchStats *stats = b->stats;

	if (stats == NULL)
		return;

	stats->batches++;
	stats->rows += rows;
	if (rows > stats->max_rows)
		stats->max_rows = rows;
	if (rows < stats->min_rows && rows > 0)
		stats->min_rows = rows;
}

double
RowBatchAvgRows(RowBatch *b)
{
	RowBatchStats *stats = b->stats;

	Assert(stats != NULL);
	if (stats->batches == 0)
		return 0.0;

	return (double) stats->rows / stats->batches;
}
