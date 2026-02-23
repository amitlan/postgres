/*-------------------------------------------------------------------------
 *
 * execRowBatch.h
 *		Executor batch envelope for passing row batch state upward
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/executor/execRowBatch.h
 *-------------------------------------------------------------------------
 */
#ifndef EXECROWBATCH_H
#define EXECROWBATCH_H

#include "executor/tuptable.h"

typedef struct RowBatchOps RowBatchOps;

/*
 * RowBatch
 *
 * Data carrier from table AM to executor. The AM populates am_payload
 * and nrows via scan_getnextbatch(). The executor calls ops->materialize_all
 * to populate slots[] when it needs tuple data.
 *
 * Selection state (which rows survived qual eval) is owned by the executor,
 * not the batch.
 */
typedef struct RowBatch
{
	void	   *am_payload;
	const RowBatchOps *ops;

	int			max_rows;			/* executor-set upper bound */
	int			nrows;				/* rows TAM put in */
	int			pos;				/* iteration position */
	bool		materialized;		/* tuples in slots valid? */

	TupleTableSlot **slots;			/* row view */
} RowBatch;

/*
 * RowBatchOps -- AM-specific operations on a RowBatch.
 *
 * Table AMs set b->ops during scan_begin_batch to provide
 * callbacks that the executor uses to access batch contents.
 *
 * Currently only materialize_into_slots is defined, which
 * populates b->slots[] from the AM's native tuple representation
 * stored in b->am_payload.  AMs that store tuple data in pinned
 * pages (e.g. heapam) use this to bind slot headers to on-page
 * tuple data without copying.
 *
 * Additional callbacks can be added here as new AMs or executor
 * features require them.
 */
typedef struct RowBatchOps
{
	void		(*materialize_into_slots) (RowBatch *b);
} RowBatchOps;

/* Create/teardown */
extern RowBatch *RowBatchCreate(TupleDesc scandesc, int max_rows);
extern void RowBatchCreateSlots(RowBatch *b, TupleDesc tupdesc,
								const TupleTableSlotOps *tts_ops);
extern void RowBatchReset(RowBatch *b, bool drop_slots);

/* Validation */
static inline bool
RowBatchIsValid(RowBatch *b)
{
	return b != NULL && b->max_rows > 0;
}

/* Iteration over materialized slots */
static inline bool
RowBatchHasMore(RowBatch *b)
{
	return b->pos < b->nrows;
}

static inline TupleTableSlot *
RowBatchGetNextSlot(RowBatch *b)
{
	return b->pos < b->nrows ? b->slots[b->pos++] : NULL;
}

static inline TupleTableSlot *
RowBatchGetSlot(RowBatch *b, int index)
{
	Assert(index < b->nrows);
	return b->slots[index];
}

static inline void
RowBatchRewind(RowBatch *b)
{
	b->pos = 0;
}

/* Materialize AM payload into slots (no-op if already done) */
static inline void
RowBatchMaterializeAll(RowBatch *b)
{
	if (b->materialized)
		return;

	Assert(b->slots != NULL);
	if (b->ops == NULL || b->ops->materialize_into_slots == NULL)
		elog(ERROR, "RowBatch has no materialize_into_slots op");

	b->ops->materialize_into_slots(b);
	b->materialized = true;
	b->pos = 0;
}

#endif	/* EXECROWBATCH_H */
