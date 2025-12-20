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

#include <limits.h>

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

	/* Statistics (populated when EXPLAIN ANALYZE BATCHES) */
	bool	track_stats;	/* whether to collect stats */
	int64	stat_batches;	/* total number of batches fetched */
	int64	stat_rows;		/* total tuples across all batches */
	int		stat_max_rows;	/* max rows in any single batch */
	int		stat_min_rows;	/* min rows in any single batch (non-zero) */
} RowBatch;

/*
 * RowBatchOps -- AM-specific helpers for lazy materialization.
 */
typedef struct RowBatchOps
{
	void (*materialize_all)(RowBatch *b,
							TupleTableSlot **dst);
} RowBatchOps;


/* Helpers */
extern RowBatch *RowBatchCreate(TupleDesc scandesc, int capacity, bool track_stats);
extern void RowBatchReset(RowBatch *b, bool drop_slots);

/* Validation */
static inline bool
RowBatchIsValid(RowBatch *b)
{
	return b != NULL && b->max_rows > 0 && b->slots != NULL;
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

	if (b->ops == NULL || b->ops->materialize_all == NULL)
		elog(ERROR, "RowBatch has no materialize_all op");

	b->ops->materialize_all(b, b->slots);
	b->materialized = true;
	b->pos = 0;
}

/* === Batching stats. ===*/

extern void RowBatchRecordStats(RowBatch *b, int rows);
extern double RowBatchAvgRows(RowBatch *b);

#endif	/* EXECROWBATCH_H */
