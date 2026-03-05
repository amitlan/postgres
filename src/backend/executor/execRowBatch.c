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
	b->slots = NULL;

	return b;
}

/*
 * RowBatchCreateSlots
 *		Allocate the slot array for a RowBatch.
 *
 * Called by table AMs during scan_begin_batch to create slots with
 * TupleTableSlotOps appropriate for the AM's tuple format.  For
 * example, heapam passes TTSOpsHeapTuple so that materialize_into_slots
 * can bind slot headers directly to on-page tuple data.
 *
 * Must be called exactly once per RowBatch lifetime -- the batch must
 * not already have slots allocated.
 *
 * The slots are owned by the RowBatch and freed by RowBatchReset()
 * when drop_slots is true.
 */
void
RowBatchCreateSlots(RowBatch *b, TupleDesc tupdesc,
					const TupleTableSlotOps *tts_ops)
{
	Assert(b->slots == NULL);

	b->slots = palloc(sizeof(TupleTableSlot *) * b->max_rows);
	for (int i = 0; i < b->max_rows; i++)
		b->slots[i] = MakeSingleTupleTableSlot(tupdesc, tts_ops);
}

/*
 * RowBatchReset
 *		Reset an existing RowBatch envelope to empty.
 */
void
RowBatchReset(RowBatch *b, bool drop_slots)
{
	Assert(b != NULL);

	if (b->slots)
	{
		for (int i = 0; i < b->max_rows; i++)
		{
			ExecClearTuple(b->slots[i]);
			if (drop_slots)
				ExecDropSingleTupleTableSlot(b->slots[i]);
		}
	}

	b->nrows = 0;
	b->pos = 0;
	b->materialized = false;
}
