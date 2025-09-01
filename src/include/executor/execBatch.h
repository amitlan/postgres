/*-------------------------------------------------------------------------
 *
 * execBatch.h
 *		Executor batch envelope for passing tuple batch state upward
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/executor/execBatch.h
 *-------------------------------------------------------------------------
 */
#ifndef EXECBATCH_H
#define EXECBATCH_H

#include "executor/tuptable.h"

/* XXX fixed 64 for PoC */
#define	EXEC_BATCH_ROWS		64

/*
 * TupleBatchOps -- AM-specific helpers for lazy materialization.
 */
typedef struct TupleBatchOps
{
	void (*materialize_all)(void *am_payload,
							TupleTableSlot **dst,
							int maxslots);
} TupleBatchOps;

/*
 * TupleBatch
 *
 * Envelope for a batch of tuples produced by a plan node (e.g., SeqScan) per
 * call to a batch variant of ExecSeqScan().
 */
typedef struct TupleBatch
{
	void	   *am_payload;
	const TupleBatchOps *ops;
	int			ntuples;				/* number of tuples in am_payload */
	bool		materialized;		 /* tuples in slots valid? */
	struct TupleTableSlot **inslots; /* slots for tuples read "into" batch */
	struct TupleTableSlot **outslots; /* slots for tuples going "out of"
									   * batch */
	struct TupleTableSlot **activeslots;
	int			maxslots;

	int		nvalid;		/* number of returnable tuples in outslots */
	int		next;		/* 0-based index of next tuple to be returned */
} TupleBatch;


/* Helpers */
extern TupleBatch *TupleBatchCreate(TupleDesc scandesc, int capacity);
extern void TupleBatchReset(TupleBatch *b, bool drop_slots);
extern void TupleBatchUseInput(TupleBatch *b, int nvalid);
extern void TupleBatchUseOutput(TupleBatch *b, int nvalid);
extern bool TupleBatchIsValid(TupleBatch *b);
extern void TupleBatchRewind(TupleBatch *b);
extern int TupleBatchGetNumValid(TupleBatch *b);

static inline TupleTableSlot *
TupleBatchGetNextSlot(TupleBatch *b)
{
	return b->next < b->nvalid ? b->activeslots[b->next++] : NULL;
}

static inline TupleTableSlot *
TupleBatchGetSlot(TupleBatch *b, int index)
{
	Assert(index < b->nvalid);
	return b->activeslots[index];
}

static inline void
TupleBatchStoreInOut(TupleBatch *b, int index, TupleTableSlot *out)
{
	Assert(TupleBatchIsValid(b));
	b->outslots[index] = out;
}

static inline bool
TupleBatchHasMore(TupleBatch *b)
{
	return b->activeslots && b->next < b->nvalid;
}

static inline void
TupleBatchMaterializeAll(TupleBatch *b)
{
	if (b->materialized)
		return;

	if (b->ops == NULL || b->ops->materialize_all == NULL)
		elog(ERROR, "TupleBatch has no slots and no materialize_all op");

	b->ops->materialize_all(b->am_payload, b->inslots, b->ntuples);
	TupleBatchUseInput(b, b->ntuples);
}

#endif	/* EXECBATCH_H */
