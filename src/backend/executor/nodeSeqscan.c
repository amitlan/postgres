/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
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
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "miscadmin.h"
#include "utils/rel.h"

static TupleTableSlot *SeqNext(SeqScanState *node);
static TupleTableSlot *ExecSeqScanNoQualNoProj(PlanState *pstate);
static TupleTableSlot *ExecSeqScanNoQual(PlanState *pstate);
static TupleTableSlot *ExecSeqScanNoProj(PlanState *pstate);
static TupleTableSlot *ExecSeqScanNoEPQ(PlanState *pstate);
static pg_attribute_always_inline TupleTableSlot *ExecSeqScanNoEPQImpl(PlanState *pstate,
																	   ExprState *qual,
																	   ProjectionInfo *projInfo);

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
static TupleTableSlot *
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
static bool
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
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) SeqNext,
					(ExecScanRecheckMtd) SeqRecheck);
}

/*
 * ExecSeqScanNoEPQImpl
 *		A specialized version of ExecScan() for use in plans where
 *		ExecInitSeqScan() determines that the node cannot be invoked by
 *		EvalPlanQual().
 *
 * This function is marked with the always-inline attribute to allow compilers
 * to create specialized versions for different combinations of qual (NULL or
 * non-NULL) and projInfo (NULL or non-NULL). By inlining, unnecessary branches
 * can be eliminated in each of the ExecSeqScan*() functions below.
 *
 * ExecInitSeqScan() assigns the appropriate variant to pstate->ExecProcNode of
 * the SeqScan node minimizing execution-time overhead for checking qual and
 * projInfo.
 */
static pg_attribute_always_inline TupleTableSlot *
ExecSeqScanNoEPQImpl(PlanState *pstate, ExprState *qual,
					 ProjectionInfo *projInfo)
{
	SeqScanState *node = castNode(SeqScanState, pstate);
	ExprContext *econtext;

	econtext = node->ss.ps.ps_ExprContext;

	/*
	 * If we have neither a qual to check nor a projection to do, just skip
	 * all the overhead and return the raw scan tuple.
	 */
	if (!qual && !projInfo)
	{
		ResetExprContext(econtext);
		return SeqNext(node);
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * get a tuple from the access method.  Loop until we obtain a tuple that
	 * passes the qualification.
	 */
	for (;;)
	{
		TupleTableSlot *slot;

		CHECK_FOR_INTERRUPTS();

		slot = SeqNext(node);

		/*
		 * if the slot returned by the accessMtd contains NULL, then it means
		 * there is nothing more to scan so we just return an empty slot,
		 * being careful to use the projection result slot so it has correct
		 * tupleDesc.
		 */
		if (TupIsNull(slot))
		{
			if (projInfo)
				return ExecClearTuple(projInfo->pi_state.resultslot);
			else
				return slot;
		}

		/*
		 * place the current tuple into the expr context
		 */
		econtext->ecxt_scantuple = slot;

		/*
		 * check that the current tuple satisfies the qual-clause
		 *
		 * check for non-null qual here to avoid a function call to ExecQual()
		 * when the qual is null ... saves only a few cycles, but they add up
		 * ...
		 */
		if (qual == NULL || ExecQual(qual, econtext))
		{
			/*
			 * Found a satisfactory scan tuple.
			 */
			if (projInfo)
			{
				/*
				 * Form a projection tuple, store it in the result tuple slot
				 * and return it.
				 */
				return ExecProject(projInfo);
			}
			else
			{
				/*
				 * Here, we aren't projecting, so just return scan tuple.
				 */
				return slot;
			}
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);
	}
}

/*
 * Variants of ExecSeqScan() used when no EvalPlanQual() is necessary,
 * specialized based on the presence of qual and projection as described in
 * the comment above ExecSeqScanNoEPQImpl().
 */
static TupleTableSlot *
ExecSeqScanNoQualNoProj(PlanState *pstate)
{
	Assert(pstate->qual == NULL && pstate->ps_ProjInfo == NULL);
	return ExecSeqScanNoEPQImpl(pstate, NULL, NULL);
}

static TupleTableSlot *
ExecSeqScanNoQual(PlanState *pstate)
{
	Assert(pstate->qual == NULL);
	return ExecSeqScanNoEPQImpl(pstate, NULL, pstate->ps_ProjInfo);
}

static TupleTableSlot *
ExecSeqScanNoProj(PlanState *pstate)
{
	Assert(pstate->ps_ProjInfo == NULL);
	return ExecSeqScanNoEPQImpl(pstate, pstate->qual, NULL);
}

static TupleTableSlot *
ExecSeqScanNoEPQ(PlanState *pstate)
{
	Assert(pstate->qual != NULL && pstate->ps_ProjInfo != NULL);
	return ExecSeqScanNoEPQImpl(pstate, pstate->qual, pstate->ps_ProjInfo);
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
						  table_slot_callbacks(scanstate->ss.ss_currentRelation));

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
	if (estate->es_epq_active != NULL)
		scanstate->ss.ps.ExecProcNode = ExecSeqScan;
	else if (scanstate->ss.ps.qual == NULL &&
			 scanstate->ss.ps.ps_ProjInfo == NULL)
		scanstate->ss.ps.ExecProcNode = ExecSeqScanNoQualNoProj;
	else if (scanstate->ss.ps.qual == NULL)
		scanstate->ss.ps.ExecProcNode = ExecSeqScanNoQual;
	else if (scanstate->ss.ps.ps_ProjInfo == NULL)
		scanstate->ss.ps.ExecProcNode = ExecSeqScanNoProj;
	else
		scanstate->ss.ps.ExecProcNode = ExecSeqScanNoEPQ;

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
