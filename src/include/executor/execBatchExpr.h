/*-------------------------------------------------------------------------
 *
 * execBatchExpr.h
 *		Batched expression evaluation over RowBatch
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/executor/execBatchExpr.h
 *-------------------------------------------------------------------------
 */
#ifndef EXECBATCHEXPR_H
#define EXECBATCHEXPR_H

#include "access/attnum.h"
#include "executor/execRowBatch.h"
#include "fmgr.h"

typedef struct ExprContext ExprContext;
typedef struct TupleTableSlot TupleTableSlot;

/* ----------------------------------------------------------------
 *		Batched Qual Evaluation
 *
 * BatchQualState is a standalone qual evaluator that processes an entire
 * RowBatch at once without touching ExprState, ExprEvalStep, or the EEOP
 * dispatch table.  Evaluation is a plain C loop per clause over the
 * survivor array.
 * ----------------------------------------------------------------
 */

typedef enum BatchQualClauseKind
{
	BQC_VAR_CONST,				/* Var op Const */
	BQC_VAR_VAR,				/* Var op Var */
	BQC_IS_NULL,				/* Var IS NULL */
	BQC_IS_NOT_NULL				/* Var IS NOT NULL */
} BatchQualClauseKind;

typedef struct BatchQualClause
{
	BatchQualClauseKind kind;

	/* Operands */
	AttrNumber	l_attno;		/* left Var attribute (always set) */
	AttrNumber	r_attno;		/* right Var attribute (BQC_VAR_VAR) */
	Datum		r_const;		/* constant datum (BQC_VAR_CONST) */
	bool		r_isnull;		/* constant is NULL? (BQC_VAR_CONST) */

	/* Comparison function */
	FmgrInfo   *finfo;			/* NULL for NullTest kinds */
	Oid			collation;		/* operator collation */
} BatchQualClause;

typedef struct BatchQualState
{
	int			nclauses;
	BatchQualClause *clauses;	/* array[nclauses] */
	AttrNumber	max_attno;		/* highest attno for deform */
} BatchQualState;

extern BatchQualState *BatchQualInit(List *qual);
extern int	BatchQualExec(BatchQualState *bqs, RowBatch *b,
						  ExprContext *econtext,
						  TupleTableSlot **outslots);

#endif	/* EXECBATCHEXPR_H */
