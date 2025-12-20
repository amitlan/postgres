/*-------------------------------------------------------------------------
 *
 * execBatch.c
 *		Helpers for TupleBatch
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/execBatch.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "executor/execBatch.h"

/*
 * TupleBatchCreate
 *		Allocate and initialize a new TupleBatch envelope.
 */
TupleBatch *
TupleBatchCreate(TupleDesc scandesc, int capacity)
{
	TupleBatch  *b;
	TupleTableSlot **inslots,
				   **outslots;

	inslots = palloc(sizeof(TupleTableSlot *) * capacity);
	outslots = palloc(sizeof(TupleTableSlot *) * capacity);
	for (int i = 0; i < capacity; i++)
		inslots[i] = MakeSingleTupleTableSlot(scandesc, &TTSOpsHeapTuple);

	b = (TupleBatch *) palloc(sizeof(TupleBatch));

	/* Initial state: empty envelope */
	b->am_payload = NULL;
	b->ntuples = 0;
	b->inslots = inslots;
	b->outslots = outslots;
	b->activeslots = NULL;
	b->outslots = outslots;
	b->maxslots = capacity;

	b->nvalid = 0;
	b->next = 0;

	return b;
}

/*
 * TupleBatchReset
 *		Reset an existing TupleBatch envelope to empty.
 */
void
TupleBatchReset(TupleBatch *b, bool drop_slots)
{
	if (b == NULL)
		return;

	for (int i = 0; i < b->maxslots; i++)
	{
		ExecClearTuple(b->inslots[i]);
		if (drop_slots)
			ExecDropSingleTupleTableSlot(b->inslots[i]);
	}

	if (drop_slots)
	{
		pfree(b->inslots);
		pfree(b->outslots);
		b->inslots = b->outslots = NULL;
	}

	b->ntuples = 0;
	b->nvalid = 0;
	b->next = 0;
	b->activeslots = NULL;
}

void
TupleBatchUseInput(TupleBatch *b, int nvalid)
{
	b->materialized = true;
	b->activeslots = b->inslots;
	b->nvalid = nvalid;
	b->next = 0;
}

void
TupleBatchUseOutput(TupleBatch *b, int nvalid)
{
	b->materialized = true;
	b->activeslots = b->outslots;
	b->nvalid = nvalid;
	b->next = 0;
}

bool
TupleBatchIsValid(TupleBatch *b)
{
	return	b != NULL &&
			b->maxslots > 0 &&
			b->inslots != NULL &&
			b->outslots != NULL;
}

void
TupleBatchRewind(TupleBatch *b)
{
	b->next = 0;
}

int
TupleBatchGetNumValid(TupleBatch *b)
{
	return b->nvalid;
}
