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
RowBatchCreate(TupleDesc scandesc, int max_rows)
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
