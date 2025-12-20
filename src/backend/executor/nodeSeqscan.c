/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanReInitializeDSM reinitialize DSM for fresh parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "executor/execRowBatch.h"
#include "executor/execScan.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "utils/rel.h"

static TupleTableSlot *SeqNext(SeqScanState *node);
static TupleTableSlot *ExecSeqScanBatchSlot(PlanState *pstate);
static TupleTableSlot *ExecSeqScanBatchSlotWithQual(PlanState *pstate);
static TupleTableSlot *ExecSeqScanBatchSlotWithProject(PlanState *pstate);
static TupleTableSlot *ExecSeqScanBatchSlotWithQualProject(PlanState *pstate);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static pg_attribute_always_inline TupleTableSlot *
SeqNext(SeqScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(node->ss.ss_currentRelation,
								   estate->es_snapshot,
								   0, NULL);
		node->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * get the next tuple from the table
	 */
	if (table_scan_getnextslot(scandesc, direction, slot))
		return slot;
	return NULL;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static pg_attribute_always_inline bool
SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple. This variant is used when there is no es_epq_active, no qual
 *		and no projection.  Passing const-NULLs for these to ExecScanExtended
 *		allows the compiler to eliminate the additional code that would
 *		ordinarily be required for the evaluation of these.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	Assert(pstate->qual == NULL);
	Assert(pstate->ps_ProjInfo == NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							NULL,
							NULL);
}

/*
 * Variant of ExecSeqScan() but when qual evaluation is required.
 */
static TupleTableSlot *
ExecSeqScanWithQual(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	/*
	 * Use pg_assume() for != NULL tests to make the compiler realize no
	 * runtime check for the field is needed in ExecScanExtended().
	 */
	Assert(pstate->state->es_epq_active == NULL);
	pg_assume(pstate->qual != NULL);
	Assert(pstate->ps_ProjInfo == NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							pstate->qual,
							NULL);
}

/*
 * Variant of ExecSeqScan() but when projection is required.
 */
static TupleTableSlot *
ExecSeqScanWithProject(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	Assert(pstate->qual == NULL);
	pg_assume(pstate->ps_ProjInfo != NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							NULL,
							pstate->ps_ProjInfo);
}

/*
 * Variant of ExecSeqScan() but when qual evaluation and projection are
 * required.
 */
static TupleTableSlot *
ExecSeqScanWithQualProject(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	pg_assume(pstate->qual != NULL);
	pg_assume(pstate->ps_ProjInfo != NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							pstate->qual,
							pstate->ps_ProjInfo);
}

/*
 * Variant of ExecSeqScan for when EPQ evaluation is required.  We don't
 * bother adding variants of this for with/without qual and projection as
 * EPQ doesn't seem as exciting a case to optimize for.
 */
static TupleTableSlot *
ExecSeqScanEPQ(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) SeqNext,
					(ExecScanRecheckMtd) SeqRecheck);
}

/* ----------------------------------------------------------------
 *						Batch Support
 * ----------------------------------------------------------------
 */

/*
 * SeqScanCanUseBatching
 *		Check whether this SeqScan can use batch mode execution.
 *
 * Batching requires: the GUC is enabled, no EPQ recheck is active, the scan
 * is forward-only, and the table AM supports batching with the current
 * snapshot (see table_supports_batching()).
 */
static bool
SeqScanCanUseBatching(SeqScanState *scanstate, int eflags)
{
	Relation	relation = scanstate->ss.ss_currentRelation;

	return	executor_batch_rows > 1 &&
			relation &&
			table_supports_batching(relation,
									scanstate->ss.ps.state->es_snapshot) &&
			!(eflags & EXEC_FLAG_BACKWARD) &&
			scanstate->ss.ps.state->es_epq_active == NULL;
}

/*
 * SeqScanInitBatching
 *		Set up batch execution state and select the appropriate
 *		ExecProcNode variant for batch mode.
 *
 * Called from ExecInitSeqScan when SeqScanCanUseBatching returns true.
 * Overwrites the ExecProcNode pointer set by the non-batch path.
 */
static void
SeqScanInitBatching(SeqScanState *scanstate)
{
	RowBatch   *batch;
	EState	   *estate = scanstate->ss.ps.state;
	bool		track_stats = estate->es_instrument &&
		(estate->es_instrument & INSTRUMENT_BATCHES);

	batch = RowBatchCreate(MaxHeapTuplesPerPage, track_stats);
	batch->slot = scanstate->ss.ss_ScanTupleSlot;
	scanstate->batch = batch;

	/* Choose batch variant */
	if (scanstate->ss.ps.qual == NULL)
	{
		if (scanstate->ss.ps.ps_ProjInfo == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScanBatchSlot;
		else
			scanstate->ss.ps.ExecProcNode = ExecSeqScanBatchSlotWithProject;
	}
	else
	{
		if (scanstate->ss.ps.ps_ProjInfo == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScanBatchSlotWithQual;
		else
			scanstate->ss.ps.ExecProcNode = ExecSeqScanBatchSlotWithQualProject;
	}
}

/*
 * SeqScanResetBatching
 *		Reset or tear down batch execution state.
 *
 * When drop is false (rescan), resets the RowBatch and releases any
 * AM-held resources like buffer pins, but keeps allocations for reuse.
 * When drop is true (end of node), frees everything.
 */
static void
SeqScanResetBatching(SeqScanState *scanstate, bool drop)
{
	RowBatch *b = scanstate->batch;

	if (b)
	{
		RowBatchReset(b, drop);
		if (b->am_payload)
		{
			if (drop)
			{
				table_scan_end_batch(scanstate->ss.ss_currentScanDesc, b);
				b->am_payload = NULL;
			}
			else
				table_scan_reset_batch(scanstate->ss.ss_currentScanDesc, b);
		}
		if (drop)
			pfree(b);
	}
}

/*
 * SeqNextBatch
 *		Fetch the next batch of tuples from the table AM.
 *
 * Lazily initializes the scan descriptor and AM batch state on first
 * call.  Returns false at end of scan.
 */
static bool
SeqNextBatch(SeqScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	RowBatch *b = node->batch;

	Assert(b != NULL);

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	Assert(ScanDirectionIsForward(direction));

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(node->ss.ss_currentRelation,
								   estate->es_snapshot,
								   0, NULL);
		node->ss.ss_currentScanDesc = scandesc;
	}

	/* Lazily create the AM batch payload. */
	if (b->am_payload == NULL)
	{
		const TableAmRoutine *tam PG_USED_FOR_ASSERTS_ONLY = scandesc->rs_rd->rd_tableam;

		Assert(tam && tam->scan_begin_batch);
		table_scan_begin_batch(scandesc, b);
	}

	if (!table_scan_getnextbatch(scandesc, b, direction))
		return false;

	RowBatchRecordStats(b, b->nrows);

	return true;
}

/*
 * SeqScanBatchSlot
 *		Core loop for batch-driven SeqScan variants.
 *
 * Internally fetches tuples in batches from the table AM, but returns
 * one slot at a time to preserve the single-slot interface expected by
 * parent nodes.  When the current batch is exhausted, fetches and
 * materializes the next one.
 *
 * qual and projInfo are passed explicitly so the compiler can eliminate
 * dead branches when inlined into the typed wrapper functions (e.g.
 * ExecSeqScanBatchSlot passes NULL for both).
 *
 * EPQ is not supported in the batch path; asserted at entry.
 */
static inline TupleTableSlot *
SeqScanBatchSlot(SeqScanState *node,
				 ExprState *qual, ProjectionInfo *projInfo)
{
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	RowBatch *b = node->batch;

	/* Batch path does not support EPQ */
	Assert(node->ss.ps.state->es_epq_active == NULL);
	Assert(RowBatchIsValid(b));

	for (;;)
	{
		TupleTableSlot *in;

		CHECK_FOR_INTERRUPTS();

		/* Get next input slot from current batch, or refill */
		if (!RowBatchHasMore(b))
		{
			if (!SeqNextBatch(node))
				return NULL;
		}

		in = RowBatchGetNextSlot(b);
		Assert(in);

		/* No qual, no projection: direct return */
		if (qual == NULL && projInfo == NULL)
			return in;

		ResetExprContext(econtext);
		econtext->ecxt_scantuple = in;

		/* Check qual if present */
		if (qual != NULL && !ExecQual(qual, econtext))
		{
			InstrCountFiltered1(node, 1);
			continue;
		}

		/* Project if needed, otherwise return scan tuple directly */
		if (projInfo != NULL)
			return ExecProject(projInfo);

		return in;
	}
}

static TupleTableSlot *
ExecSeqScanBatchSlot(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	Assert(pstate->qual == NULL);
	Assert(pstate->ps_ProjInfo == NULL);

	return SeqScanBatchSlot(node, NULL, NULL);
}

static TupleTableSlot *
ExecSeqScanBatchSlotWithQual(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	/*
	 * Use pg_assume() for != NULL tests to make the compiler realize no
	 * runtime check for the field is needed in ExecScanExtended().
	 */
	Assert(pstate->state->es_epq_active == NULL);
	pg_assume(pstate->qual != NULL);
	Assert(pstate->ps_ProjInfo == NULL);

	return SeqScanBatchSlot(node, pstate->qual, NULL);
}

/*
 * Variant of ExecSeqScan() but when projection is required.
 */
static TupleTableSlot *
ExecSeqScanBatchSlotWithProject(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	Assert(pstate->qual == NULL);
	pg_assume(pstate->ps_ProjInfo != NULL);

	return SeqScanBatchSlot(node, NULL, pstate->ps_ProjInfo);
}

/*
 * Variant of ExecSeqScan() but when qual evaluation and projection are
 * required.
 */
static TupleTableSlot *
ExecSeqScanBatchSlotWithQualProject(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	pg_assume(pstate->qual != NULL);
	pg_assume(pstate->ps_ProjInfo != NULL);

	return SeqScanBatchSlot(node, pstate->qual, pstate->ps_ProjInfo);
}

/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	SeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SeqScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation
	 */
	scanstate->ss.ss_currentRelation =
		ExecOpenScanRelation(estate,
							 node->scan.scanrelid,
							 eflags);

	/* and create slot with the appropriate rowtype */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(scanstate->ss.ss_currentRelation),
						  table_slot_callbacks(scanstate->ss.ss_currentRelation),
						  TTS_FLAG_OBEYS_NOT_NULL_CONSTRAINTS);

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);

	/*
	 * When EvalPlanQual() is not in use, assign ExecProcNode for this node
	 * based on the presence of qual and projection. Each ExecSeqScan*()
	 * variant is optimized for the specific combination of these conditions.
	 */
	if (scanstate->ss.ps.state->es_epq_active != NULL)
		scanstate->ss.ps.ExecProcNode = ExecSeqScanEPQ;
	else if (scanstate->ss.ps.qual == NULL)
	{
		if (scanstate->ss.ps.ps_ProjInfo == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScan;
		else
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithProject;
	}
	else
	{
		if (scanstate->ss.ps.ps_ProjInfo == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithQual;
		else
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithQualProject;
	}

	if (SeqScanCanUseBatching(scanstate, eflags))
		SeqScanInitBatching(scanstate);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndSeqScan(SeqScanState *node)
{
	TableScanDesc scanDesc;

	/*
	 * get information from node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	SeqScanResetBatching(node, true);

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
		table_endscan(scanDesc);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node)
{
	TableScanDesc scan;

	scan = node->ss.ss_currentScanDesc;

	if (scan != NULL)
		table_rescan(scan,		/* scan desc */
					 NULL);		/* new scan keys */

	SeqScanResetBatching(node, false);
	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanEstimate(SeqScanState *node,
					ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = table_parallelscan_estimate(node->ss.ss_currentRelation,
												  estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeDSM(SeqScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelTableScanDesc pscan;

	pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);
	table_parallelscan_initialize(node->ss.ss_currentRelation,
								  pscan,
								  estate->es_snapshot);
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanReInitializeDSM(SeqScanState *node,
						   ParallelContext *pcxt)
{
	ParallelTableScanDesc pscan;

	pscan = node->ss.ss_currentScanDesc->rs_parallel;
	table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeWorker(SeqScanState *node,
							ParallelWorkerContext *pwcxt)
{
	ParallelTableScanDesc pscan;

	pscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}
