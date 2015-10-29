/*-------------------------------------------------------------------------
 *
 * partitioning.c
 *	  Planner support routines for partitioned tables
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/partitioning.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "optimizer/clauses.h"
#include "optimizer/partitioning.h"
#include "optimizer/planmain.h"
#include "utils/lsyscache.h"
#include "utils/partition.h"
#include "utils/syscache.h"

/*
 * PartKeyClauseSet
 *		Data structure for collecting qual clause info that match
 *		partition key of a given partitoned rel
 */
typedef struct PartKeyClauseSet
{
	bool	nonempty;	/* True if lists are not all empty */
	bool	has_pseudoconstant;

	/*
	 * Lists of clause operand expressions and operators therein, one per
	 * key column. List for a given key column consists of operands from
	 * all the clauses (and operators therein) that reference the column.
	 */
	List   *clauseoperands[PARTITION_MAX_KEYS];
	List   *clauseoperators[PARTITION_MAX_KEYS];
} PartKeyClauseSet;

static void match_clauses_to_partition_key(PartitionOptInfo	*poinfo,
							List *clauses,
							PartKeyClauseSet *clauseset);
static void match_clause_to_partition_key(PartitionOptInfo *poinfo,
							RestrictInfo *rinfo,
							PartKeyClauseSet *clauseset);
static bool match_clause_to_partkeycol(PartitionOptInfo *poinfo,
							int partkeycol,
							RestrictInfo *rinfo,
							PartKeyClauseSet *clauseset);
static bool match_partkeycol_to_operand(Node *operand, int partkeycol,
							PartitionOptInfo *poinfo);
static bool is_operator_ok(Oid expr_op, Oid opfamily,
							bool partkeycol_on_left);

/*
 * get_rel_partitions
 *		Find relevant partitions of rel considering baserestrictinfo
 */
List *
get_rel_partitions(RelOptInfo *rel, RangeTblEntry *rte)
{
	Relation			relation;
	List			   *result = NIL;
	PartitionOptInfo   *poinfo = rel->partoptinfo;
	List			   *baserestrictinfo = rel->baserestrictinfo;
	PartKeyClauseSet	clauseset;
	int					partkeycol;
	EState			   *estate;

	int		nclauses[PARTITION_MAX_KEYS];
	Datum  *clauseval[PARTITION_MAX_KEYS];
	Oid	   *clauseop[PARTITION_MAX_KEYS];

	relation = heap_open(rte->relid, NoLock);

	/*
	 * The following both matches individual clauses to the partition key
	 * and also populates clauseoperands and clauseoperators of clauseset.
	 * They are lists, per key column, of operand expression trees and
	 * corresponding opnos, respectively. It is assumed that members of
	 * each list come from OpExpr clauses that are logically ANDed. Note
	 * that we do not bother to recognize anything other than plain
	 * operator clauses. That excludes boolean operator clauses.
	 */
	MemSet(&clauseset, 0, sizeof(clauseset));
	match_clauses_to_partition_key(poinfo,
								   baserestrictinfo,
								   &clauseset);

	/*
	 * If there is a pseudoconstant restrictinfo in the list, produce
	 * an empty partition list to force the caller to mark this rel
	 * as dummy.
	 */
	if (clauseset.has_pseudoconstant)
	{
		heap_close(relation, NoLock);
		return NIL;
	}

	/*
	 * No relevant clauses were found, so simply return all partitions.
	 */
	if (!clauseset.nonempty)
	{
		result = get_all_partitions(relation);

		heap_close(relation, NoLock);
		return result;
	}

	/*
	 * Found some clauses referencing partition key column(s).
	 *
	 * Evaluate operand expressions and populate datum arrays for respective
	 * key columns. The idea is to find, for a given key column, all the
	 * partitions for which comparison op(operand_datum, bound_datum) would
	 * return true where op is the respective operator. Remember that clauses
	 * were suitably commuted as necessary.
	 */
	estate = CreateExecutorState();

	for (partkeycol = 0; partkeycol < poinfo->ncolumns; partkeycol++)
	{
		List	   *clauseoperands = clauseset.clauseoperands[partkeycol];
		List	   *clauseoperators = clauseset.clauseoperators[partkeycol];
		ListCell   *lc1, *lc2;
		int			i, n = list_length(clauseoperands);
		MemoryContext	oldcontext;

		clauseval[partkeycol] = (Datum *) palloc0(sizeof(Datum) * n);
		clauseop[partkeycol] = (Oid *) palloc0(sizeof(Oid) * n);

		oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

		i = 0;
		forboth(lc1, clauseoperands, lc2, clauseoperators)
		{
			Expr   *clause_operand = (Expr *) lfirst(lc1);
			Oid		clause_operator = lfirst_oid(lc2);
			ExprState  *operand_state;
			Datum		operand_val;
			bool		isNull;

			operand_state = ExecInitExpr(clause_operand, NULL);
			operand_val = ExecEvalExprSwitchContext(operand_state,
									   GetPerTupleExprContext(estate),
									   &isNull, NULL);

			/*
			 * The operand evaluated to null, skip as a clause without any
			 * significance as regards its usefulness for partition-pruning.
			 */
			if (isNull)
				continue;

			clauseval[partkeycol][i] = operand_val;
			clauseop[partkeycol][i] = clause_operator;
			i++;
		}

		nclauses[partkeycol] = i;

		MemoryContextSwitchTo(oldcontext);
	}

	result = get_partitions_for_clauses(relation,
								nclauses, clauseval, clauseop);

	FreeExecutorState(estate);
	heap_close(relation, NoLock);

	return result;
}

/*
 * match_clauses_to_partition_key
 *		Add clauses matching with partition key to *clauseset.
 */
static void
match_clauses_to_partition_key(PartitionOptInfo	*poinfo, List *clauses,
							   PartKeyClauseSet *clauseset)
{
	ListCell   *lc;

	foreach(lc, clauses)
	{
		RestrictInfo   *rinfo = (RestrictInfo *) lfirst(lc);

		Assert(IsA(rinfo, RestrictInfo));
		match_clause_to_partition_key(poinfo, rinfo, clauseset);
	}
}

/*
 * match_clause_to_partition_key
 *		Test whether rinfo can be used against the partition key
 *
 * If rinfo is usable, mark clauseset as non-empty. Const operand and
 * operator are added into respective list by match_clause_to_partkeycol().
 * *clauseset must be zero-initialized before the first call.
 */
static void
match_clause_to_partition_key(PartitionOptInfo *poinfo,
							  RestrictInfo *rinfo,
							  PartKeyClauseSet *clauseset)
{
	int		partkeycol;

	for (partkeycol = 0; partkeycol < poinfo->ncolumns; partkeycol++)
	{
		if (match_clause_to_partkeycol(poinfo, partkeycol, rinfo, clauseset))
		{
			clauseset->nonempty = true;
			return;
		}
	}
}

/*
 * match_clause_to_partkeycol
 *		Test whether rinfo matches partition key column (partkeycol)
 *
 * If it does, evalulate Const operand and store both the result and
 * operator to respective list in clauseset.
 */
static bool
match_clause_to_partkeycol(PartitionOptInfo *poinfo,
							int partkeycol,
							RestrictInfo *rinfo,
							PartKeyClauseSet *clauseset)
{
	Expr	   *clause = rinfo->clause;
	Index		relid = poinfo->rel->relid;
	Oid			opfamily = poinfo->opfamily[partkeycol];
	Node	   *leftop, *rightop;
	Relids		left_relids, right_relids;
	Oid			expr_op;
	bool		plain_op;

	if (rinfo->pseudoconstant)
	{
		clauseset->has_pseudoconstant = true;
		return false;
	}

	/* Match only simple opclauses now */
	if (is_opclause(clause))
	{
		leftop = get_leftop(clause);
		rightop = get_rightop(clause);
		if (!leftop || !rightop)
			return false;
		left_relids = rinfo->left_relids;
		right_relids = rinfo->right_relids;
		expr_op = ((OpExpr *) clause)->opno;
		plain_op = true;
	}
	else
		return false;

	if (match_partkeycol_to_operand(leftop, partkeycol, poinfo) &&
		!bms_is_member(relid, right_relids) &&
		IsA(rightop, Const))
	{
		if (is_operator_ok(expr_op, opfamily, true))
		{
			int	origlen = list_length(clauseset->clauseoperands[partkeycol]);

			clauseset->clauseoperands[partkeycol] =
				list_append_unique_ptr(clauseset->clauseoperands[partkeycol],
									   rightop);

			/*
			 * If clause operand was found to be a duplicate, we didn't add
			 * it again, in which case, also do not add the operator oid.
			 */
			if (list_length(clauseset->clauseoperands[partkeycol]) > origlen)
				clauseset->clauseoperators[partkeycol] =
					lappend_oid(clauseset->clauseoperators[partkeycol], expr_op);

			return true;
		}
	}

	if (plain_op &&
		match_partkeycol_to_operand(rightop, partkeycol, poinfo) &&
		!bms_is_member(relid, left_relids) &&
		IsA(leftop, Const))
	{
		if (is_operator_ok(expr_op, opfamily, false))
		{
			int		origlen = list_length(clauseset->clauseoperands[partkeycol]);
			Oid		commuted_expr_op = get_commutator(expr_op);

			clauseset->clauseoperands[partkeycol] =
				list_append_unique_ptr(clauseset->clauseoperands[partkeycol],
									   leftop);

			/*
			 * If clause operand was found to be a duplicate, we didn't add
			 * it again, in which case also do not add the operator oid.
			 */
			if (list_length(clauseset->clauseoperands[partkeycol]) > origlen)
				clauseset->clauseoperators[partkeycol] =
					lappend_oid(clauseset->clauseoperators[partkeycol], commuted_expr_op);

			return true;
		}
	}

	return false;
}

/*
 * match_partkeycol_to_operand
 *		Generalized test for a match between a partition key column
 *		and the operand on one side of a restriction clause.
 */
static bool
match_partkeycol_to_operand(Node *operand, int partkeycol,
							PartitionOptInfo *poinfo)
{
	int		partattno;

	if (operand && IsA(operand, RelabelType))
		operand = (Node *) ((RelabelType *) operand)->arg;

	partattno = poinfo->partkeys[partkeycol];

	if (partattno != 0)
	{
		/* Simple column reference */
		if (operand && IsA(operand, Var) &&
			poinfo->rel->relid == ((Var *) operand)->varno &&
			partattno == ((Var *) operand)->varattno)
			return true;
	}
	else
	{
		/* Partition key experession */
		ListCell   *partexpr_item;
		int			i;
		Node	   *partexpr;

		partexpr_item = list_head(poinfo->partexprs);
		for (i = 0; i < partkeycol; i++)
		{
			if (poinfo->partkeys[i] == 0)
			{
				if (partexpr_item == NULL)
					elog(ERROR, "wrong number of partition key expressions");
				partexpr_item = lnext(partexpr_item);
			}
		}

		if (partexpr_item == NULL)
			elog(ERROR, "wrong number of partition key expressions");

		partexpr = (Node *) lfirst(partexpr_item);

		if (partexpr && IsA(partexpr, RelabelType))
			partexpr = (Node *) ((RelabelType *) partexpr)->arg;

		if (equal(partexpr, operand))
			return true;
	}

	return false;
}

/*
 * is_operator_ok
 *		Does the operator match specified (partkey) opfamily?
 *
 * If the partkey is on the right, what we actually want to know
 * is whether the operator has a commutator operator that matches
 * the opfamily.
 */
static bool
is_operator_ok(Oid expr_op, Oid opfamily, bool partkeycol_on_left)
{
	/* Get the commuted operator if necessary */
	if (!partkeycol_on_left)
	{
		expr_op = get_commutator(expr_op);
		if (expr_op == InvalidOid)
			return false;
	}

	/* OK if the (commuted) operator is a member of the specified opfamily */
	return op_in_opfamily(expr_op, opfamily);
}
