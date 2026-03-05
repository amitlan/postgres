/*-------------------------------------------------------------------------
 *
 * execRowBatch.h
 *		Executor batch envelope for passing row batch state upward
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
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
typedef struct RowBatchStats RowBatchStats;

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

	TupleTableSlot *slot;			/* row view */

	RowBatchStats *stats;			/* NULL if instrumentation stats
									 * are not requested */
} RowBatch;

/*
 * RowBatchOps -- AM-specific operations on a RowBatch.
 *
 * Table AMs set b->ops during scan_begin_batch to provide
 * callbacks that the executor uses to access batch contents.
 *
 * repoint_slot re-points the batch's single slot to the tuple at
 * index idx within the current batch.  The slot remains valid until
 * the next call or until the batch is exhausted.
 *
 * Additional callbacks can be added here as new AMs or executor
 * features require them.
 */
typedef struct RowBatchOps
{
	void		(*repoint_slot) (RowBatch *b, int idx);
} RowBatchOps;

/* Instrumentation stats populated for EXPLAIN ANALYZE BATCHES */
typedef struct RowBatchStats
{
	int64	batches;	/* total number of batches fetched */
	int64	rows;		/* total tuples across all batches */
	int		max_rows;	/* max rows in any single batch */
	int		min_rows;	/* min rows in any single batch (non-zero) */
} RowBatchStats;

/* Create/teardown */
extern RowBatch *RowBatchCreate(int max_rows, bool track_stats);
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
	if (b->pos >= b->nrows)
		return NULL;
	b->ops->repoint_slot(b, b->pos++);
	return b->slot;
}

/* Returns the tuple at specified pos */
static inline TupleTableSlot *
RowBatchGetSlot(RowBatch *b, int pos)
{
	if (pos >= b->nrows)
		return NULL;
	b->ops->repoint_slot(b, pos);
	return b->slot;
}
/* === Batching stats. ===*/

extern void RowBatchRecordStats(RowBatch *b, int rows);
extern double RowBatchAvgRows(RowBatch *b);

#endif	/* EXECROWBATCH_H */
