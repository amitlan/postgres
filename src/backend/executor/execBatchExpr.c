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

#include "catalog/pg_operator_d.h"
#include "catalog/pg_proc.h"
#include "executor/executor.h"
#include "executor/execBatchExpr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
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

static BatchCmpFn lookup_batch_cmpfn(Oid opfuncid);

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
		 * Walker guarantees strict + leakproof.  Try the fast-path
		 * lookup first; fall back to fmgr if no direct function exists.
		 */
		cl->cmpfn = lookup_batch_cmpfn(op->opfuncid);
		if (cl->cmpfn == NULL)
		{
			cl->finfo = palloc(sizeof(FmgrInfo));
			fmgr_info(op->opfuncid, cl->finfo);
		}
		else
			cl->finfo = NULL;

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

							if (cl->cmpfn)
								pass = cl->cmpfn(lv, cl->r_const);
							else
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

							if (cl->cmpfn)
								pass = cl->cmpfn(lv, rv);
							else
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

/* ---- Fast-path comparison functions in execRowBatch.c ---- */

/* --- int2 --- */

static bool
batch_int2eq(Datum lv, Datum rv)
{
	return DatumGetInt16(lv) == DatumGetInt16(rv);
}

static bool
batch_int2ne(Datum lv, Datum rv)
{
	return DatumGetInt16(lv) != DatumGetInt16(rv);
}

static bool
batch_int2lt(Datum lv, Datum rv)
{
	return DatumGetInt16(lv) < DatumGetInt16(rv);
}

static bool
batch_int2le(Datum lv, Datum rv)
{
	return DatumGetInt16(lv) <= DatumGetInt16(rv);
}

static bool
batch_int2gt(Datum lv, Datum rv)
{
	return DatumGetInt16(lv) > DatumGetInt16(rv);
}

static bool
batch_int2ge(Datum lv, Datum rv)
{
	return DatumGetInt16(lv) >= DatumGetInt16(rv);
}

/* --- int4 --- */

static bool
batch_int4eq(Datum lv, Datum rv)
{
	return DatumGetInt32(lv) == DatumGetInt32(rv);
}

static bool
batch_int4ne(Datum lv, Datum rv)
{
	return DatumGetInt32(lv) != DatumGetInt32(rv);
}

static bool
batch_int4lt(Datum lv, Datum rv)
{
	return DatumGetInt32(lv) < DatumGetInt32(rv);
}

static bool
batch_int4le(Datum lv, Datum rv)
{
	return DatumGetInt32(lv) <= DatumGetInt32(rv);
}

static bool
batch_int4gt(Datum lv, Datum rv)
{
	return DatumGetInt32(lv) > DatumGetInt32(rv);
}

static bool
batch_int4ge(Datum lv, Datum rv)
{
	return DatumGetInt32(lv) >= DatumGetInt32(rv);
}

/* --- int8 --- */

static bool
batch_int8eq(Datum lv, Datum rv)
{
	return DatumGetInt64(lv) == DatumGetInt64(rv);
}

static bool
batch_int8ne(Datum lv, Datum rv)
{
	return DatumGetInt64(lv) != DatumGetInt64(rv);
}

static bool
batch_int8lt(Datum lv, Datum rv)
{
	return DatumGetInt64(lv) < DatumGetInt64(rv);
}

static bool
batch_int8le(Datum lv, Datum rv)
{
	return DatumGetInt64(lv) <= DatumGetInt64(rv);
}

static bool
batch_int8gt(Datum lv, Datum rv)
{
	return DatumGetInt64(lv) > DatumGetInt64(rv);
}

static bool
batch_int8ge(Datum lv, Datum rv)
{
	return DatumGetInt64(lv) >= DatumGetInt64(rv);
}

/* --- float4 --- */

static bool
batch_float4eq(Datum lv, Datum rv)
{
	float4 a = DatumGetFloat4(lv);
	float4 b = DatumGetFloat4(rv);
	return float4_cmp_internal(a, b) == 0;
}

static bool
batch_float4ne(Datum lv, Datum rv)
{
	float4 a = DatumGetFloat4(lv);
	float4 b = DatumGetFloat4(rv);
	return float4_cmp_internal(a, b) != 0;
}

static bool
batch_float4lt(Datum lv, Datum rv)
{
	float4 a = DatumGetFloat4(lv);
	float4 b = DatumGetFloat4(rv);
	return float4_cmp_internal(a, b) < 0;
}

static bool
batch_float4le(Datum lv, Datum rv)
{
	float4 a = DatumGetFloat4(lv);
	float4 b = DatumGetFloat4(rv);
	return float4_cmp_internal(a, b) <= 0;
}

static bool
batch_float4gt(Datum lv, Datum rv)
{
	float4 a = DatumGetFloat4(lv);
	float4 b = DatumGetFloat4(rv);
	return float4_cmp_internal(a, b) > 0;
}

static bool
batch_float4ge(Datum lv, Datum rv)
{
	float4 a = DatumGetFloat4(lv);
	float4 b = DatumGetFloat4(rv);
	return float4_cmp_internal(a, b) >= 0;
}

/* --- float8 --- */

static bool
batch_float8eq(Datum lv, Datum rv)
{
	float8 a = DatumGetFloat8(lv);
	float8 b = DatumGetFloat8(rv);
	return float8_cmp_internal(a, b) == 0;
}

static bool
batch_float8ne(Datum lv, Datum rv)
{
	float8 a = DatumGetFloat8(lv);
	float8 b = DatumGetFloat8(rv);
	return float8_cmp_internal(a, b) != 0;
}

static bool
batch_float8lt(Datum lv, Datum rv)
{
	float8 a = DatumGetFloat8(lv);
	float8 b = DatumGetFloat8(rv);
	return float8_cmp_internal(a, b) < 0;
}

static bool
batch_float8le(Datum lv, Datum rv)
{
	float8 a = DatumGetFloat8(lv);
	float8 b = DatumGetFloat8(rv);
	return float8_cmp_internal(a, b) <= 0;
}

static bool
batch_float8gt(Datum lv, Datum rv)
{
	float8 a = DatumGetFloat8(lv);
	float8 b = DatumGetFloat8(rv);
	return float8_cmp_internal(a, b) > 0;
}

static bool
batch_float8ge(Datum lv, Datum rv)
{
	float8 a = DatumGetFloat8(lv);
	float8 b = DatumGetFloat8(rv);
	return float8_cmp_internal(a, b) >= 0;
}

/* --- int24 cross-type --- */

static bool
batch_int24eq(Datum lv, Datum rv)
{
	return (int32) DatumGetInt16(lv) == DatumGetInt32(rv);
}

static bool
batch_int24lt(Datum lv, Datum rv)
{
	return (int32) DatumGetInt16(lv) < DatumGetInt32(rv);
}

static bool
batch_int24gt(Datum lv, Datum rv)
{
	return (int32) DatumGetInt16(lv) > DatumGetInt32(rv);
}

/* --- int42 cross-type --- */

static bool
batch_int42eq(Datum lv, Datum rv)
{
	return DatumGetInt32(lv) == (int32) DatumGetInt16(rv);
}

static bool
batch_int42lt(Datum lv, Datum rv)
{
	return DatumGetInt32(lv) < (int32) DatumGetInt16(rv);
}

static bool
batch_int42gt(Datum lv, Datum rv)
{
	return DatumGetInt32(lv) > (int32) DatumGetInt16(rv);
}

/* --- int48 cross-type --- */

static bool
batch_int48eq(Datum lv, Datum rv)
{
	return (int64) DatumGetInt32(lv) == DatumGetInt64(rv);
}

static bool
batch_int48lt(Datum lv, Datum rv)
{
	return (int64) DatumGetInt32(lv) < DatumGetInt64(rv);
}

static bool
batch_int48gt(Datum lv, Datum rv)
{
	return (int64) DatumGetInt32(lv) > DatumGetInt64(rv);
}

/* --- int84 cross-type --- */

static bool
batch_int84eq(Datum lv, Datum rv)
{
	return DatumGetInt64(lv) == (int64) DatumGetInt32(rv);
}

static bool
batch_int84lt(Datum lv, Datum rv)
{
	return DatumGetInt64(lv) < (int64) DatumGetInt32(rv);
}

static bool
batch_int84gt(Datum lv, Datum rv)
{
	return DatumGetInt64(lv) > (int64) DatumGetInt32(rv);
}

/* --- Oid --- */

static bool
batch_oideq(Datum lv, Datum rv)
{
	return DatumGetObjectId(lv) == DatumGetObjectId(rv);
}

static bool
batch_oidne(Datum lv, Datum rv)
{
	return DatumGetObjectId(lv) != DatumGetObjectId(rv);
}

/* --- bool --- */

static bool
batch_booleq(Datum lv, Datum rv)
{
	return DatumGetBool(lv) == DatumGetBool(rv);
}

static bool
batch_boolne(Datum lv, Datum rv)
{
	return DatumGetBool(lv) != DatumGetBool(rv);
}


/*
 * Lookup table mapping operator function OIDs to direct batch comparison
 * functions.  Sorted by OID for binary search if the table grows large,
 * but linear scan is fine for ~40 entries.
 */
typedef struct BatchCmpEntry
{
	Oid			opfuncid;
	BatchCmpFn	fn;
} BatchCmpEntry;

static const BatchCmpEntry batch_cmp_table[] =
{
	/* int2 */
	{F_INT2EQ, batch_int2eq},
	{F_INT2NE, batch_int2ne},
	{F_INT2LT, batch_int2lt},
	{F_INT2LE, batch_int2le},
	{F_INT2GT, batch_int2gt},
	{F_INT2GE, batch_int2ge},

	/* int4 */
	{F_INT4EQ, batch_int4eq},
	{F_INT4NE, batch_int4ne},
	{F_INT4LT, batch_int4lt},
	{F_INT4LE, batch_int4le},
	{F_INT4GT, batch_int4gt},
	{F_INT4GE, batch_int4ge},

	/* int8 */
	{F_INT8EQ, batch_int8eq},
	{F_INT8NE, batch_int8ne},
	{F_INT8LT, batch_int8lt},
	{F_INT8LE, batch_int8le},
	{F_INT8GT, batch_int8gt},
	{F_INT8GE, batch_int8ge},

	/* float4 */
	{F_FLOAT4EQ, batch_float4eq},
	{F_FLOAT4NE, batch_float4ne},
	{F_FLOAT4LT, batch_float4lt},
	{F_FLOAT4LE, batch_float4le},
	{F_FLOAT4GT, batch_float4gt},
	{F_FLOAT4GE, batch_float4ge},

	/* float8 */
	{F_FLOAT8EQ, batch_float8eq},
	{F_FLOAT8NE, batch_float8ne},
	{F_FLOAT8LT, batch_float8lt},
	{F_FLOAT8LE, batch_float8le},
	{F_FLOAT8GT, batch_float8gt},
	{F_FLOAT8GE, batch_float8ge},

	/* int2/int4 cross-type */
	{F_INT24EQ, batch_int24eq},
	{F_INT24LT, batch_int24lt},
	{F_INT24GT, batch_int24gt},
	{F_INT42EQ, batch_int42eq},
	{F_INT42LT, batch_int42lt},
	{F_INT42GT, batch_int42gt},

	/* int4/int8 cross-type */
	{F_INT48EQ, batch_int48eq},
	{F_INT48LT, batch_int48lt},
	{F_INT48GT, batch_int48gt},
	{F_INT84EQ, batch_int84eq},
	{F_INT84LT, batch_int84lt},
	{F_INT84GT, batch_int84gt},

	/* oid */
	{F_OIDEQ, batch_oideq},
	{F_OIDNE, batch_oidne},

	/* bool */
	{F_BOOLEQ, batch_booleq},
	{F_BOOLNE, batch_boolne},
};

#define BATCH_CMP_TABLE_SIZE (sizeof(batch_cmp_table) / sizeof(batch_cmp_table[0]))

/*
 * lookup_batch_cmpfn
 *		Find a direct comparison function for the given operator function OID.
 *
 * Returns NULL if no fast-path is available; caller falls back to fmgr.
 */
static BatchCmpFn
lookup_batch_cmpfn(Oid opfuncid)
{
	for (int i = 0; i < BATCH_CMP_TABLE_SIZE; i++)
	{
		if (batch_cmp_table[i].opfuncid == opfuncid)
			return batch_cmp_table[i].fn;
	}
	return NULL;
}
