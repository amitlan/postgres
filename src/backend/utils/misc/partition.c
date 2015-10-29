/*-------------------------------------------------------------------------
 *
 * partition.c
 *        Partitioning related utility functions.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/backend/utils/misc/partition.c
 *
 *-------------------------------------------------------------------------
*/
#include "postgres.h"

#include "catalog/heap.h"
#include "catalog/pg_collation.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/partition.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"

#define compare_using_func(func, val1, val2)\
							DatumGetInt32(FunctionCall2Coll(&(func),\
										  DEFAULT_COLLATION_OID,\
										  (val1), (val2)))

static Oid list_partition_for_tuple(Relation rel, PartitionDesc *pdesc,
								Datum key);
static Oid range_partition_for_tuple(Relation rel, PartitionDesc *pdesc,
								Datum *key);
static List *list_partitions_for_clauses(Relation rel,
								PartitionDesc *pdesc,
								int *nclauses,
								Datum **clauseval,
								Oid **clauseop);
static List *range_partitions_for_clauses(Relation rel,
								PartitionDesc *pdesc,
								int *nclauses,
								Datum **clauseval,
								Oid **clauseop);
static List *make_oid_list_from_array(Oid *arr, int start, int n);
static int32 rangemaxs_cmp_datum(const Datum *vals,
							const Datum *rangemaxs,
							int partnatts,
							FmgrInfo *partsupfunc);
static int32 rangemaxs_cmp_datum0(const Datum *vals,
							const Datum *rangemaxs,
							int partnatts,
							FmgrInfo *partsupfunc);

typedef int32 (*bsearch_compare_fn) (const Datum *vals,
							const Datum *rangemaxs,
							int partnatts,
							FmgrInfo *partsupfunc);
static int range_minmax_bsearch(PartitionDesc *pdesc,
							FmgrInfo *partsupfunc,
							int partnatts,
							const Datum *key,
							bsearch_compare_fn cmp_fn);
static int range_max_bsearch(PartitionDesc *pdesc,
							FmgrInfo *partsupfunc,
							int partnatts,
							const Datum *key,
							bsearch_compare_fn cmp_fn,
							bool *exact_match);


/*
 * rangemaxs_cmp_datum
 *		Compare datum against rangemaxs of a range partition
 *
 * vals and rangemaxs are each 'natts' datums long. partsupfunc is an array
 * of pointers to FmgrInfo structs, one per partition key column.
 */
static int32
rangemaxs_cmp_datum(const Datum *vals, const Datum *rangemaxs,
							  int partnatts, FmgrInfo *partsupfunc)
{
	int 	i;
	int32	result;

	for (i = 0; i < partnatts; i++)
	{
		result = compare_using_func(partsupfunc[i], vals[i], rangemaxs[i]);

		/* consider a multi-column key */
		if (!result)
			continue;
		else
			return result;
	}

	return 0;
}

/*
 * rangemaxs_cmp_datum0
 *		Compare datum against rangemaxs of a range partition (single column
 *		version)
 *
 * Currently used by range partition pruning function that considers only
 * the clauses that reference the first key column.
 */
static int32
rangemaxs_cmp_datum0(const Datum *vals, const Datum *rangemaxs,
							int partnatts, FmgrInfo *partsupfunc)
{
	Assert(partnatts == 1);

	return compare_using_func(partsupfunc[0], vals[0], rangemaxs[0]);
}

/*
 * list_partition_overlaps_with_existing
 *		Check whether list partition overlaps with existing partitions of rel
 *
 * If it does overlap, return the oid of the partition it overlaps with.
 */
bool
list_partition_overlaps(Relation rel, Datum *listvalues,
						int listnvalues,
						Oid *overlapsWith)
{
	int			i, j, k;
	int32		result;
	FmgrInfo		*partsupfunc = rel->rd_partsupfunc;
	PartitionDesc   *pdesc = RelationGetPartitionDesc(rel);

	for (i = 0; i < pdesc->numparts; i++)
	{
		int		ilistnvalues = pdesc->listnvalues[i];
		Datum  *ilistvalues = pdesc->listvalues[i];
		Oid		iOid = pdesc->oids[i];

		for (j = 0; j < listnvalues; j++)
		{
			Datum	newDatum = listvalues[j];

			for (k = 0; k < ilistnvalues; k++)
			{
				result = compare_using_func(partsupfunc[0],
											newDatum,
											ilistvalues[k]);
				if (!result)
				{
					*overlapsWith = iOid;
					return true;
				}
			}
		}
	}

	return false;
}

/*
 * range_partition_empty
 *		Check whether range partition is empty
 *
 * This amounts to comparing the new rangemaxs with that of the last partition.
 * A new range partition should always be defined to have a rangemax that is
 * greater than that of the last partition.
 */
bool
range_partition_empty(Relation rel, Datum *rangemaxs)
{
	int			i;
	int32		cmpval;
	Datum	   *rel_rangemaxs;
	int			partnatts = rel->rd_partkey->partnatts;
	FmgrInfo   *partsupfunc = rel->rd_partsupfunc;
	PartitionDesc *pdesc = RelationGetPartitionDesc(rel);

	if (pdesc->numparts < 1)
		return false;

	/* Get ahold of rangemaxs of the last partition of rel */
	rel_rangemaxs = (Datum *) palloc0(partnatts * sizeof(Datum));

	for (i = 0; i < partnatts; i++)
		rel_rangemaxs[i] = pdesc->rangemaxs[i][pdesc->numparts - 1];

	cmpval = rangemaxs_cmp_datum(rangemaxs,
								 rel_rangemaxs,
								 partnatts,
								 partsupfunc);

	pfree(rel_rangemaxs);
	return cmpval == 0;
}

/*
 * range_partition_overlaps
 *		Check whether range partition overlaps with some existing partition
 *		of rel
 *
 * This amounts to comparing the new rangemaxs with that of the last partition.
 * A new range partition should always be defined to have a rangemax that is
 * greater than that of the last partition.
 */
bool
range_partition_overlaps(Relation rel, Datum *rangemaxs)
{
	int			i;
	int32		cmpval;
	Datum	   *rel_rangemaxs;
	int			partnatts = rel->rd_partkey->partnatts;
	FmgrInfo   *partsupfunc = rel->rd_partsupfunc;
	PartitionDesc *pdesc = RelationGetPartitionDesc(rel);

	if (pdesc->numparts < 1)
		return false;

	/* Get ahold of rangemaxs of the last partition of rel */
	rel_rangemaxs = (Datum *) palloc0(partnatts * sizeof(Datum));

	for (i = 0; i < partnatts; i++)
		rel_rangemaxs[i] = pdesc->rangemaxs[i][pdesc->numparts - 1];

	cmpval = rangemaxs_cmp_datum(rangemaxs,
								 rel_rangemaxs,
								 partnatts,
								 partsupfunc);

	pfree(rel_rangemaxs);
	return cmpval < 0;
}

/*
 * get_partition_for_tuple
 *		Find the partition for tuple (in slot)
 */
Oid
get_partition_for_tuple(Relation rel,
						PartitionKeyInfo *pkinfo,
						PartitionDesc *pdesc,
						TupleTableSlot *slot,
						EState *estate)
{
	int		i;
	char	strategy = rel->rd_partkey->partstrat;
	Oid		targetOid,
			relid = RelationGetRelid(rel);
	Datum	key[PARTITION_MAX_KEYS];
	bool	isnull[PARTITION_MAX_KEYS];

	if (pdesc->numparts < 1)
		return InvalidOid;

	/* Extract partition key from tuple */
	Assert(GetPerTupleExprContext(estate)->ecxt_scantuple == slot);
	FormPartitionKeyDatum(pkinfo,
						  slot,
						  estate,
						  key,
						  isnull);

	/* Disallow nulls in partition key cols */
	for (i = 0; i < pkinfo->pi_NumKeyAttrs; i++)
		if (isnull[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value for partition key column \"%s\" not allowed",
							get_attname(relid, pkinfo->pi_KeyAttrNumbers[i]))));

	/* Find out actual partition. */
	switch (strategy)
	{
		case PARTITION_STRAT_LIST:
			targetOid = list_partition_for_tuple(rel, pdesc, key[0]);
			break;

		case PARTITION_STRAT_RANGE:
			targetOid = range_partition_for_tuple(rel, pdesc, key);
			break;
	}

	return targetOid;
}

/*
 * list_partition_for_tuple
 *		Find the list partition for a tuple (using value in key)
 *
 * There is single datum to compare because the partition key in case of
 * list partitioning consists of only one column.
 */
static Oid
list_partition_for_tuple(Relation rel, PartitionDesc *pdesc, Datum key)
{
	int			i,
				j;
	int32		result;
	FmgrInfo	cmpfn = rel->rd_partsupfunc[0];

	Assert(pdesc->numparts > 0);

	for (i = 0; i < pdesc->numparts; i++)
	{
		for (j = 0; j < pdesc->listnvalues[i]; j++)
		{
			result = compare_using_func(cmpfn,
										pdesc->listvalues[i][j],
										key);

			if (!result)
				return pdesc->oids[i];
		}
	}

	return InvalidOid;
}

/*
 * range_partition_for_tuple
 *		Find the range partition for a tuple (using values in key)
 *
 * Range partitioning supports multi-column key, so the key may contain
 * more than one datum. Comparison function rangemaxs_cmp_datum can
 * deal with that.
 */
static Oid
range_partition_for_tuple(Relation rel, PartitionDesc *pdesc, Datum *key)
{
	int			partnatts = rel->rd_partkey->partnatts;
	FmgrInfo   *partsupfunc = rel->rd_partsupfunc;
	int			partidx;

	Assert(pdesc->numparts > 0);

	partidx = range_minmax_bsearch(pdesc,
								   partsupfunc,
								   partnatts,
								   key,
								   rangemaxs_cmp_datum);

	if (partidx >= 0)
		return pdesc->oids[partidx];

	return InvalidOid;
}

/*
 * get_all_partitions
 *		Return all partitions of rel
 */
List *
get_all_partitions(Relation rel)
{
	List   *result = NIL;
	PartitionDesc  *pdesc = RelationGetPartitionDesc(rel);

	if (pdesc->numparts > 0)
		result =  make_oid_list_from_array(pdesc->oids, 0, pdesc->numparts);

	pfree(pdesc);
	return result;
}

/*
 * make_oid_list_from_array
 *		Build list of OIDs from array of the same
 */
static List *
make_oid_list_from_array(Oid *arr, int start, int n)
{
	int		i;
	List   *result = NIL;

	for (i = 0; i < n; i++)
		result = lappend_oid(result, arr[start+i]);

	return result;
}

/*
 * get_partitions_from_clauses
 *		Return relevant partitions of rel considering clause values
 *
 * Arrays nclauses, clauseval and clauseop each contain partnatts elements.
 * nclauses[i] refers to the number of clauses matched to i'th partition
 * key column. clauseval[i] points to the array of that many datums which
 * correspond to values of Const operands from those clauses and clauseop[i]
 * points to the array of opnos from the respective clauses.
 *
 * It would be nice to invent a better structure to pass this around.
 */
List *
get_partitions_for_clauses(Relation rel, int *nclauses, Datum **clauseval,
						   Oid **clauseop)
{
	List   *result = NIL;
	char	strategy = rel->rd_partkey->partstrat;
	PartitionDesc  *pdesc = RelationGetPartitionDesc(rel);

	if (pdesc->numparts > 0)
	{
		switch(strategy)
		{
			case PARTITION_STRAT_LIST:
				result = list_partitions_for_clauses(rel,
													 pdesc,
													 nclauses,
													 clauseval,
													 clauseop);
				break;

			case PARTITION_STRAT_RANGE:
				result = range_partitions_for_clauses(rel,
													  pdesc,
													  nclauses,
													  clauseval,
													  clauseop);
				break;
		}
	}

	pfree(pdesc);
	return result;
}

/*
 * list_partitions_from_clauses
 *		Find the list partition containing the value in clause
 */
static List *
list_partitions_for_clauses(Relation rel,
							PartitionDesc *pdesc,
							int *nclauses,
							Datum **clauseval,
							Oid **clauseop)
{
	List	   *result = NIL;
	int			i, j, k,
				ncolumns = rel->rd_partkey->partnatts;

	Oid		   *opfamily = rel->rd_partopfamily;
	FmgrInfo   *partsupfunc = rel->rd_partsupfunc;

	/* Because LIST */
	Assert(pdesc->numparts > 0);
	Assert(ncolumns == 1);

	/*
	 * If there is a clause with non-equality operator, we cannot do much
	 * further below.
	 */
	for (j = 0; j < nclauses[0]; j++)
	{
		Oid		operator = clauseop[0][j];
		int		clauseop_strategy;
		Oid		clauseop_lefttype;
		Oid		clauseop_righttype;

		get_op_opfamily_properties(operator, opfamily[0], false,
									&clauseop_strategy,
									&clauseop_lefttype,
									&clauseop_righttype);

		if (clauseop_strategy != BTEqualStrategyNumber)
			return make_oid_list_from_array(pdesc->oids, 0, pdesc->numparts);
	}

	/*
	 * We want every matching partition to satisfy all available clauses on
	 * a given column. In reality, there should only ever be a single clause
	 * to even have reached here because more than one equality clause
	 * involving a given column would have been reduced to pseudo-constant
	 * FALSE qual and hence dealt with by higher level code.
	 */
	for (i = 0; i < pdesc->numparts; i++)
	{
		int		n_matched_clauses = 0;

		/* There is only one column - 0th */
		for (j = 0; j < nclauses[0]; j++)
		{
			bool		found = false;

			for (k = 0; k < pdesc->listnvalues[i]; k++)
			{
				Datum	cmpval;

				cmpval = compare_using_func(partsupfunc[0],
											clauseval[0][j],
											pdesc->listvalues[i][k]);

				/* If matched, move on to the next clause */
				if (!cmpval)
				{
					found = true;
					break;
				}
			}

			if (found)
				++n_matched_clauses;
		}

		if (n_matched_clauses == nclauses[0])
			result = lappend_oid(result, pdesc->oids[i]);
	}

	return result;
}

/*
 * range_partitions_from_clauses
 *		Find range partitions containing values in the requested range
 *
 * XXX - Currently, only the clauses referencing the first key column are
 * considered.
 */
static List *
range_partitions_for_clauses(Relation rel,
							 PartitionDesc *pdesc,
							 int *nclauses,
							 Datum **clauseval,
							 Oid **clauseop)
{
	int		idx, range_start, range_end;
	int		i, j;
	Datum	clause_eq[PARTITION_MAX_KEYS],
			clause_min[PARTITION_MAX_KEYS],
			clause_max[PARTITION_MAX_KEYS];
	bool	clause_eq_found[PARTITION_MAX_KEYS],
			clause_min_found[PARTITION_MAX_KEYS],
			clause_max_found[PARTITION_MAX_KEYS];
	bool	clause_min_incl[PARTITION_MAX_KEYS],
			clause_max_incl[PARTITION_MAX_KEYS];
	int			ncolumns = rel->rd_partkey->partnatts;
	Oid		   *opfamily = rel->rd_partopfamily;
	FmgrInfo   *partsupfunc = rel->rd_partsupfunc;

	Assert(pdesc->numparts > 0);

	/*
	 * For each column, determine clause_eq, clause_min and clause_max with
	 * exactly one value per column. They would define clausevalues that a
	 * given partition key column should be equal, greater than (or equal)
	 * and less than (or equal) to, respectively.
	 */
	MemSet(clause_eq, 0, sizeof(clause_eq));
	MemSet(clause_min, 0, sizeof(clause_min));
	MemSet(clause_max, 0, sizeof(clause_max));
	MemSet(clause_eq_found, 0, sizeof(clause_eq_found));
	MemSet(clause_min_found, 0, sizeof(clause_eq_found));
	MemSet(clause_max_found, 0, sizeof(clause_max_found));
	MemSet(clause_min_incl, 0, sizeof(clause_min_incl));
	MemSet(clause_max_incl, 0, sizeof(clause_max_incl));

	for (i = 0; i < ncolumns; i++)
	{
		for (j = 0; j < nclauses[i]; j++)
		{
			Oid		operator = clauseop[i][j];
			int		op_strategy;
			Oid		op_lefttype;
			Oid		op_righttype;

			get_op_opfamily_properties(operator, opfamily[i], false,
										&op_strategy,
										&op_lefttype,
										&op_righttype);
			switch (op_strategy)
			{
				case BTEqualStrategyNumber:
					clause_eq_found[i] = true;
					clause_eq[i] = clauseval[i][j];
					break;

				case BTGreaterEqualStrategyNumber:
				case BTGreaterStrategyNumber:
					if (clause_min_found[i])
					{
						if (compare_using_func(partsupfunc[i],
											   clauseval[i][j],
											   clause_min[i]) > 0)
						{
							clause_min[i] = clauseval[i][j];
							clause_min_incl[i] =
								(op_strategy == BTGreaterEqualStrategyNumber);
						}
					}
					else
					{
						clause_min[i] = clauseval[i][j];
						clause_min_incl[i] =
								(op_strategy == BTGreaterEqualStrategyNumber);
						clause_min_found[i] = true;
					}
					break;

				case BTLessStrategyNumber:
				case BTLessEqualStrategyNumber:
					if (clause_max_found[i])
					{
						if (compare_using_func(partsupfunc[i],
											   clauseval[i][j],
											   clause_max[i]) < 0)
						{
							clause_max[i] = clauseval[i][j];
							clause_max_incl[i] =
								(op_strategy == BTLessEqualStrategyNumber);
						}
					}
					else
					{
						clause_max[i] = clauseval[i][j];
						clause_max_incl[i] =
								(op_strategy == BTLessEqualStrategyNumber);
						clause_max_found[i] = true;
					}
					break;
			}
		}
	}

	if (clause_eq_found[0])
	{
		bool	clause_min_refutes = false,
				clause_max_refutes = false;
		bool	exact_match = false;
		int		start = 0, end = -1;

		/* Check if either clause_min or clause_max refutes clause_eq */
		if (clause_min_found[0])
		{
			if (clause_min_incl[0])
				clause_min_refutes |= compare_using_func(partsupfunc[0],
														 clause_eq[0],
														 clause_min[0]) < 0;
			else
				clause_min_refutes |= compare_using_func(partsupfunc[0],
														 clause_eq[0],
														 clause_min[0]) <= 0;
		}

		if (clause_max_found[0])
		{
			if (clause_max_incl[0])
				clause_max_refutes |= compare_using_func(partsupfunc[0],
														 clause_eq[0],
														 clause_max[0]) > 0;
			else
				clause_max_refutes |= compare_using_func(partsupfunc[0],
														 clause_eq[0],
														 clause_max[0]) >= 0;
		}

		if (clause_min_refutes || clause_max_refutes)
			return NIL;

		idx = range_max_bsearch(pdesc,
								partsupfunc,
								1,
								clause_eq,
								rangemaxs_cmp_datum0,
								&exact_match);

		if (idx >= 0)
		{
			/*
			 * When using a multi-column partition key, more than one
			 * consecutive partitions would have the same rangemax values for
			 * leading columns save the last. We need to determine index of
			 * the first and the last such partition.
			 */
			if (ncolumns > 1)
			{
				i = idx;
				while (i >= 0 && !compare_using_func(partsupfunc[0],
													 clause_eq[0],
													 pdesc->rangemaxs[0][i]))
					--i;

				start = i < idx ? ++i : i;

				i = idx;
				while (i <= pdesc->numparts - 1 &&
								!compare_using_func(partsupfunc[0],
													clause_eq[0],
													pdesc->rangemaxs[0][i]))
					++i;

				end = i > idx ? --i : i;
			}
			else if (exact_match)
			{
				if (idx < pdesc->numparts - 1)
					start = end = idx + 1;
			}
			else
				start = end = idx;
		}
		else
			return NIL;

		if (start <= end)
			return make_oid_list_from_array(pdesc->oids,
											start,
											end - start + 1);
		else
			return NIL;
	}

	range_start = 0;
	range_end = pdesc->numparts - 1;

	if (clause_min_found[0])
	{
		bool	exact_match = false;
		int		start = 0, end = -1;

		idx = range_max_bsearch(pdesc,
								partsupfunc,
								1,
								clause_min,
								rangemaxs_cmp_datum0,
								&exact_match);

		if (idx >= 0)
		{
			/* As noted above for a multi-column partition key */
			if (ncolumns > 1)
			{
				i = idx;
				while (i >= 0 && !compare_using_func(partsupfunc[0],
													 clause_min[0],
													 pdesc->rangemaxs[0][i]))
					--i;

				start = i < idx ? ++i : i;

				i = idx;
				while (i <= pdesc->numparts - 1 &&
								!compare_using_func(partsupfunc[0],
													clause_min[0],
													pdesc->rangemaxs[0][i]))
					++i;

				end = i > idx ? --i : i;

				range_start = clause_min_incl[0] ? start : end + 1;
			}
			else if (exact_match)
				range_start = idx + 1;
			else
				range_start = idx;
		}
		else
			range_start = pdesc->numparts;
	}

	if (clause_max_found[0])
	{
		bool	exact_match = false;
		int		start = 0, end = -1;

		idx = range_max_bsearch(pdesc,
								partsupfunc,
								1,
								clause_max,
								rangemaxs_cmp_datum0,
								&exact_match);

		if (idx >= 0)
		{
			/* As noted above for a multi-column partition key */
			if (ncolumns > 1)
			{
				i = idx;
				while (i >= 0 && !compare_using_func(partsupfunc[0],
													 clause_max[0],
													 pdesc->rangemaxs[0][i]))
					--i;

				start = i < idx ? ++i : i;

				i = idx;
				while (i <= pdesc->numparts - 1 &&
								!compare_using_func(partsupfunc[0],
													clause_max[0],
													pdesc->rangemaxs[0][i]))
					++i;

				end = i > idx ? --i : i;

				range_end = clause_max_incl[0] ? end : start - 1;
			}
			else if (exact_match && clause_max_incl[0] && idx < pdesc->numparts - 1)
				range_end = idx + 1;
			else
				range_end = idx;
		}
		else
			range_end = pdesc->numparts - 1;
	}

	if (range_start <= range_end)
		return make_oid_list_from_array(pdesc->oids, range_start,
												range_end - range_start + 1);

	return NIL;
}

/*
 * range_minmax_bsearch
 *		Search for partition such that left.rangemax <= key < rangemax
 *
 * Returns index of the so found partition or -1 if key is found to be
 * greater than the rangemax of the last partition.
 */
static int
range_minmax_bsearch(PartitionDesc *pdesc,
					 FmgrInfo *partsupfunc,
					 int partnatts,
					 const Datum *key,
					 bsearch_compare_fn cmp_fn)
{
	int32	cmpval;
	int		low, high, idx, i, result;
	Datum  *rangemaxs;

	rangemaxs = (Datum *) palloc0(partnatts * sizeof(Datum));

	/* Good ol' bsearch */
	low = 0;
	high = pdesc->numparts - 1;
	result = -1;
	while (low <= high)
	{
		idx = (low + high) / 2;

		for (i = 0; i < partnatts; i++)
			rangemaxs[i] = pdesc->rangemaxs[i][idx];

		cmpval = cmp_fn(key, rangemaxs, partnatts, partsupfunc);
		if (cmpval < 0)
		{
			if (idx == 0)
			{
				/* Nothing on left */
				result = idx;
				break;
			}
			else
			{
				for (i = 0; i < partnatts; i++)
					rangemaxs[i] = pdesc->rangemaxs[i][idx-1];

				cmpval = cmp_fn(key, rangemaxs, partnatts, partsupfunc);

				if (cmpval >= 0)
				{
					result = idx;
					break;
				}
				else
					high = idx - 1;
			}
		}
		else
			low = idx + 1;
	}

	pfree(rangemaxs);
	return result;
}

/*
 * range_max_bsearch
 *		Search for partition such that left.rangemax <= key < rangemax
 *
 * Returns index of the so found partition or -1 if key is found to be
 * greater than the rangemax of the last partition.
 *
 * *exact_match - informs caller whether key matches exactly with rangemax
 *				  at the returned index
 */

static int
range_max_bsearch(PartitionDesc *pdesc,
				  FmgrInfo *partsupfunc,
				  int partnatts,
				  const Datum *key,
				  bsearch_compare_fn cmp_fn,
				  bool *exact_match)
{
	int32	cmpval;
	int		low, high, idx, i, result;
	Datum  *rangemaxs;

	rangemaxs = (Datum *) palloc0(partnatts * sizeof(Datum));

	/* Good ol' bsearch */
	low = 0;
	high = pdesc->numparts - 1;
	result = -1;
	while (low <= high)
	{
		idx = (low + high) / 2;

		for (i = 0; i < partnatts; i++)
			rangemaxs[i] = pdesc->rangemaxs[i][idx];

		cmpval = cmp_fn(key, rangemaxs, partnatts, partsupfunc);

		if (cmpval < 0)
		{
			if (idx == 0)
			{
				/* Nothing on left of the first partition */
				result = idx;
				break;
			}
			else
			{
				for (i = 0; i < partnatts; i++)
					rangemaxs[i] = pdesc->rangemaxs[i][idx-1];

				cmpval = cmp_fn(key, rangemaxs, partnatts, partsupfunc);

				if (cmpval >= 0)
				{
					if (cmpval == 0)
					{
						*exact_match = true;
						result = idx - 1;
					}
					else
						result = idx;

					break;
				}
				else
					high = idx - 1;
			}
		}
		else if (cmpval == 0)
		{
			result = idx;
			*exact_match = true;
			break;
		}
		else
			low = idx + 1;
	}

	pfree(rangemaxs);
	return result;
}
