/*-------------------------------------------------------------------------
 *
 * execBatchExpr.c
 *		Batched expression evaluation over RowBatch
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/execBatchExpr.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_proc.h"
#include "executor/executor.h"
#include "executor/execBatchExpr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* ----------------------------------------------------------------
 *		Batched Qual Evaluation
 * ----------------------------------------------------------------
 */

/*
 * Extract Var attno from expression, unwrapping RelabelType.
 * Returns attno > 0 on success, 0 on failure.  System attributes
 * (attno < 0) are rejected because batch eval accesses tts_values[]
 * directly by attno, which doesn't work for system columns.
 */
static AttrNumber
extract_var_attno(Expr *expr)
{
	if (expr == NULL)
		return 0;
	if (IsA(expr, RelabelType))
		return extract_var_attno((Expr *) ((RelabelType *) expr)->arg);
	if (IsA(expr, Var) && ((Var *) expr)->varattno > 0)
		return ((Var *) expr)->varattno;
	return 0;
}

/*
 * Context for qual_batchable_walker.
 */
typedef struct QualBatchWalkerContext
{
	List	   *leaves;			/* collected leaf nodes */
	AttrNumber	max_attno;		/* highest referenced attribute */
	bool		ok;				/* stays true while batchable */
} QualBatchWalkerContext;

/*
 * qual_batchable_walker
 *		Walk qual tree, validate each node is batch-eligible, and collect
 *		leaf predicates.
 *
 * Eligible: AND-only tree of NullTest(Var), binary OpExpr(Var op Const)
 * or OpExpr(Var op Var) where operator is strict + leakproof.
 */
static bool
qual_batchable_walker(Node *node, void *context)
{
	QualBatchWalkerContext *cxt = (QualBatchWalkerContext *) context;

	if (node == NULL || !cxt->ok)
		return false;

	switch (nodeTag(node))
	{
		case T_List:
			return expression_tree_walker(node, qual_batchable_walker, cxt);

		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;

				if (b->boolop != AND_EXPR)
				{
					cxt->ok = false;
					return true;
				}
				return expression_tree_walker(node,
											  qual_batchable_walker, cxt);
			}

		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;
				AttrNumber	attno = extract_var_attno(nt->arg);

				if (attno == 0)
				{
					cxt->ok = false;
					return true;
				}
				if (attno > cxt->max_attno)
					cxt->max_attno = attno;
				cxt->leaves = lappend(cxt->leaves, node);
				return false;
			}

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				AttrNumber	lattno,
							rattno;

				if (list_length(op->args) != 2)
				{
					cxt->ok = false;
					return true;
				}

				/*
				 * Only strict operators are safe because we short-circuit
				 * on NULL inputs without calling the function.  Only
				 * leakproof operators are safe because batching changes
				 * evaluation order -- a non-leakproof operator could
				 * observe rows that should have been filtered by a
				 * security barrier qual evaluated later.
				 */
				if (!func_strict(op->opfuncid))
				{
					cxt->ok = false;
					return true;
				}
				if (!get_func_leakproof(op->opfuncid))
				{
					cxt->ok = false;
					return true;
				}

				lattno = extract_var_attno(linitial(op->args));
				if (lattno == 0)
				{
					cxt->ok = false;
					return true;
				}
				if (lattno > cxt->max_attno)
					cxt->max_attno = lattno;

				if (!IsA(lsecond(op->args), Const))
				{
					rattno = extract_var_attno(lsecond(op->args));
					if (rattno == 0)
					{
						cxt->ok = false;
						return true;
					}
					if (rattno > cxt->max_attno)
						cxt->max_attno = rattno;
				}

				cxt->leaves = lappend(cxt->leaves, node);
				return false;
			}

		default:
			cxt->ok = false;
			break;
	}

	return true;
}

/*
 * Build a BatchQualClause from a validated leaf node.
 * Returns false on unexpected structure (shouldn't happen after walker).
 */
static bool
build_clause(Node *node, BatchQualClause *cl)
{
	memset(cl, 0, sizeof(BatchQualClause));

	if (IsA(node, NullTest))
	{
		NullTest   *nt = (NullTest *) node;

		cl->kind = (nt->nulltesttype == IS_NULL) ? BQC_IS_NULL
												  : BQC_IS_NOT_NULL;
		cl->l_attno = extract_var_attno(nt->arg);
		cl->finfo = NULL;
		return (cl->l_attno > 0);
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;
		Expr	   *larg = linitial(op->args);
		Expr	   *rarg = lsecond(op->args);

		cl->l_attno = extract_var_attno(larg);
		if (cl->l_attno == 0)
			return false;

		if (IsA(rarg, Const))
		{
			Const  *c = (Const *) rarg;

			cl->kind = BQC_VAR_CONST;
			cl->r_const = c->constvalue;
			cl->r_isnull = c->constisnull;
			cl->r_attno = 0;
		}
		else
		{
			cl->kind = BQC_VAR_VAR;
			cl->r_attno = extract_var_attno(rarg);
			if (cl->r_attno == 0)
				return false;
		}

		cl->collation = exprInputCollation((Node *) op);

		/*
		 * Walker guarantees strict + leakproof.  We don't store a
		 * separate strict flag; the null check in BatchQualExec is
		 * unconditional for all operator clauses.
		 */
		cl->finfo = palloc(sizeof(FmgrInfo));
		fmgr_info(op->opfuncid, cl->finfo);

		return true;
	}

	return false;
}

/*
 * BatchQualInit
 *		Decompose a qual list into an array of BatchQualClauses.
 *
 * Returns NULL if any clause cannot be decomposed -- caller should use
 * per-tuple ExecQual in that case.
 */
BatchQualState *
BatchQualInit(List *qual)
{
	QualBatchWalkerContext cxt;
	BatchQualState *bqs;
	ListCell   *lc;
	int			i;

	if (qual == NIL)
		return NULL;

	cxt.leaves = NIL;
	cxt.max_attno = 0;
	cxt.ok = true;

	qual_batchable_walker((Node *) qual, &cxt);

	if (!cxt.ok || cxt.leaves == NIL)
		return NULL;

	bqs = palloc(sizeof(BatchQualState));
	bqs->nclauses = list_length(cxt.leaves);
	bqs->clauses = palloc(sizeof(BatchQualClause) * bqs->nclauses);
	bqs->max_attno = cxt.max_attno;

	i = 0;
	foreach(lc, cxt.leaves)
	{
		if (!build_clause((Node *) lfirst(lc), &bqs->clauses[i]))
		{
			pfree(bqs->clauses);
			pfree(bqs);
			return NULL;
		}
		i++;
	}

	return bqs;
}

/*
 * BatchQualExec
 *		Evaluate all clauses over a materialized RowBatch.
 *
 * Deforms all slots up to max_attno, then runs each clause as a tight
 * loop over the survivor set, compacting outslots in place.
 *
 * Switches to per-tuple memory context for the duration so that any
 * allocations by comparison functions (e.g. detoasting) land in
 * resettable memory.  The context is reset once at entry, not per
 * row -- total allocations are bounded by max_rows per batch.
 *
 * Returns the number of qualifying rows.  Caller iterates
 * outslots[0 .. nqualified-1].
 */
int
BatchQualExec(BatchQualState *bqs, RowBatch *b,
			  ExprContext *econtext,
			  TupleTableSlot **outslots)
{
	MemoryContext oldContext;
	int			nrows = b->nrows;
	int			kept = nrows;

	Assert(b->materialized);

	/* Reset per-batch expression memory */
	ResetExprContext(econtext);

	/*
	 * Switch to per-tuple memory context so any allocations made by
	 * comparison functions (e.g. detoasting) land in resettable memory.
	 * "Per-tuple" is really per-batch here; bounded by max_rows.
	 */
	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/*
	 * Deform all slots to the highest attribute any clause needs and
	 * seed the output array with all rows.
	 */
	for (int i = 0; i < nrows; i++)
	{
		slot_getsomeattrs(b->slots[i], bqs->max_attno);
		outslots[i] = b->slots[i];
	}

	/* Evaluate each clause, compacting survivors in place */
	for (int c = 0; c < bqs->nclauses; c++)
	{
		BatchQualClause *cl = &bqs->clauses[c];
		int			dst = 0;

		CHECK_FOR_INTERRUPTS();
		for (int i = 0; i < kept; i++)
		{
			TupleTableSlot *slot = outslots[i];
			bool		pass;

			switch (cl->kind)
			{
				case BQC_IS_NULL:
					pass = slot->tts_isnull[cl->l_attno - 1];
					break;

				case BQC_IS_NOT_NULL:
					pass = !slot->tts_isnull[cl->l_attno - 1];
					break;

				case BQC_VAR_CONST:
					{
						bool	ln = slot->tts_isnull[cl->l_attno - 1];

						/* we only allow strict operators */
						if (ln || cl->r_isnull)
							pass = false;
						else
						{
							Datum	lv = slot->tts_values[cl->l_attno - 1];

							pass = DatumGetBool(
								FunctionCall2Coll(cl->finfo,
												  cl->collation,
												  lv,
												  cl->r_const));
						}
					}
					break;

				case BQC_VAR_VAR:
					{
						bool	ln = slot->tts_isnull[cl->l_attno - 1];
						bool	rn = slot->tts_isnull[cl->r_attno - 1];

						/* we only allow strict operators */
						if (ln || rn)
							pass = false;
						else
						{
							Datum	lv = slot->tts_values[cl->l_attno - 1];
							Datum	rv = slot->tts_values[cl->r_attno - 1];

							pass = DatumGetBool(
								FunctionCall2Coll(cl->finfo,
												  cl->collation,
												  lv, rv));
						}
					}
					break;

				default:
					pass = true;	/* shouldn't happen */
					break;
			}

			if (pass)
				outslots[dst++] = slot;
		}

		kept = dst;

		if (kept == 0)
			break;				/* short-circuit: no survivors */
	}

	MemoryContextSwitchTo(oldContext);

	return kept;
}
