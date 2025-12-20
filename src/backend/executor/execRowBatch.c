/*-------------------------------------------------------------------------
 *
 * execRowBatch.c
 *		Helpers for RowBatch
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
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
RowBatchCreate(TupleDesc scandesc, int max_rows, bool track_stats)
{
	RowBatch  *b;
	TupleTableSlot **slots;
	Size		alloc_size;

	Assert(max_rows > 0);

	/* Single allocation for RowBatch + slots array */
	alloc_size = sizeof(RowBatch) + sizeof(TupleTableSlot *) * max_rows;
	b = palloc(alloc_size);
	slots = (TupleTableSlot **) ((char *) b + sizeof(RowBatch));

	for (int i = 0; i < max_rows; i++)
		slots[i] = MakeSingleTupleTableSlot(scandesc, &TTSOpsHeapTuple);

	b->am_payload = NULL;
	b->ops = NULL;
	b->max_rows = max_rows;
	b->nrows = 0;
	b->pos = 0;
	b->materialized = false;
	b->slots = slots;

	b->track_stats = track_stats;
	b->stat_batches = 0;
	b->stat_rows = 0;
	b->stat_max_rows = 0;
	b->stat_min_rows = INT_MAX;

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

	for (int i = 0; i < b->max_rows; i++)
	{
		ExecClearTuple(b->slots[i]);
		if (drop_slots)
			ExecDropSingleTupleTableSlot(b->slots[i]);
	}

	b->nrows = 0;
	b->pos = 0;
	b->materialized = false;
}

void
RowBatchRecordStats(RowBatch *b, int rows)
{
	if (!b->track_stats)
		return;

	b->stat_batches++;
	b->stat_rows += rows;
	if (rows > b->stat_max_rows)
		b->stat_max_rows = rows;
	if (rows < b->stat_min_rows && rows > 0)
		b->stat_min_rows = rows;
}

double
RowBatchAvgRows(RowBatch *b)
{
	if (b->stat_batches == 0)
		return 0.0;

	return (double) b->stat_rows / b->stat_batches;
}
