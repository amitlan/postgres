/*-------------------------------------------------------------------------
 *
 * pg_partition.c
 *	  Routines to support manipulation of the pg_partition relation.
 *
 * Note: currently, this module only contains inquiry functions; the actual
 * creation and deletion of pg_partition entries is done in heap.c. Perhaps
 * someday that code should be moved here.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_partition.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "nodes/execnodes.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"
#include "utils/typcache.h"

static int	oid_cmp(const void *p1, const void *p2);

/*
 * get_partition_parent
 *		Returns OID of the parent of relation with OID relid
 */
Oid
get_partition_parent(Oid relid)
{
	HeapTuple			tuple;
	Form_pg_partition	form;

	tuple = SearchSysCache1(PARTITIONID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for partition entry of relation %u", relid);

	form  = (Form_pg_partition) GETSTRUCT(tuple);
	Assert(form->partparent != InvalidOid);

	ReleaseSysCache(tuple);
	return form->partparent;
}

/*
 * find_partitions
 *		Returns a list of OIDs of partitions of relation with OID relid
 *
 * Partitions are locked in an order that follows the sorted order of
 * their OIDs.
 */
List *
find_partitions(Oid relid, LOCKMODE lockmode)
{
	List		   *list = NIL;
	Relation		partitionRel;
	SysScanDesc		scan;
	ScanKeyData		skey[1];
	HeapTuple		tuple;
	Oid				partrelid;
	Oid			   *oidarr;
	int				maxoids,
					numoids,
					i;

	/* Quick exit if the relation is not a partitioned table */
	if (!is_partitioned(relid))
		return NIL;

	/* Scan pg_partition and build a working array of partition OIDs */
	maxoids = 64;
	oidarr = (Oid *) palloc(maxoids * sizeof(Oid));
	numoids = 0;

	partitionRel = heap_open(PartitionRelationId, AccessShareLock);
	ScanKeyInit(&skey[0],
				Anum_pg_partition_partparent,
				BTEqualStrategyNumber, F_OIDEQ,
				relid);

	scan = systable_beginscan(partitionRel, PartitionParentIndexId, true,
								NULL, 1, skey);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		partrelid = ((Form_pg_partition) GETSTRUCT(tuple))->partrelid;
		if (numoids >= maxoids)
		{
			maxoids *= 2;
			oidarr = (Oid *) repalloc(oidarr, maxoids * sizeof(Oid));
		}
		oidarr[numoids++] = partrelid;
	}

	systable_endscan(scan);

	heap_close(partitionRel, AccessShareLock);

	/*
	 * If we found more than one partition, sort them by OID. This is important
	 * since we need to be sure all backends lock partitions in the same order
	 * to avoid needless deadlocks.
	 */
	if (numoids > 1)
		qsort(oidarr, numoids, sizeof(Oid), oid_cmp);

	/* Acquire locks and build the result list. */
	for (i = 0; i < numoids; i++)
	{
		partrelid = oidarr[i];

		/* XXX - whole locking business below may be useless after all */
		if (lockmode != NoLock)
		{
			/* Get the lock to synchronize against concurrent drop */
			LockRelationOid(partrelid, lockmode);

			/*
			 * Now that we have the lock, double-check to see if the relation
			 * really exists or not.  If not, assume it was dropped while we
			 * waited to acquire lock, and ignore it.
			 */
			if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(partrelid)))
			{
				/* Release useless lock */
				UnlockRelationOid(partrelid, lockmode);
				/* And ignore this relation */
				continue;
			}
		}

		list = lappend_oid(list, partrelid);
	}

	pfree(oidarr);

	return list;
}

/*
 * free_partitions
 *		Free the PartitionBoundInfo array (typically the one that was built
 *		using GetPartitionBounds)
 */
void
free_partitions(PartitionBoundInfo **p, int count)
{
	int	i;

	for (i = 0; i < count; i++)
	{
		if (p[i]->rangemaxs)
			pfree(p[i]->rangemaxs);
		if (p[i]->listvalues)
			pfree(p[i]->listvalues);
		pfree(p[i]);
	}

	pfree(p);
}

/*
 * GetPartitionBounds
 *		Return bound info of partitions of rel
 *
 * Returned array consists of a PartitionBoundInfo struct for every partition
 */
PartitionBoundInfo **
GetPartitionBounds(Relation rel, int *numparts)
{
	List	   *partoids;
	int			i;
	int			partnatts = rel->rd_partkey->partnatts;
	ListCell   *cell;
	PartitionKeyTypeInfo *typinfo;
	PartitionBoundInfo **partitions;

	partoids = find_partitions(RelationGetRelid(rel), NoLock);

	*numparts = list_length(partoids);

	if (*numparts < 1)
		return NULL;

	typinfo = get_key_type_info(rel);
	partitions = (PartitionBoundInfo **) palloc0(*numparts *
										sizeof(PartitionBoundInfo *));
	i = 0;
	foreach(cell, partoids)
	{
		HeapTuple			tuple;
		Form_pg_partition	form;
		int					j;
		int					rangenmaxs;
		Datum			   *rangemaxs;
		Datum				datum;
		bool				isnull;
		Oid 				partrelid;
		PartitionBoundInfo *result = NULL;

		partrelid = lfirst_oid(cell);

		tuple = SearchSysCache1(PARTITIONID, partrelid);

		/* If no tuple found, it means the entry was just dropped. */
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for partition %u", partrelid);

		result = (PartitionBoundInfo *) palloc0(sizeof(PartitionBoundInfo));
		result->oid = partrelid;
		result->partnatts = partnatts;

		form = (Form_pg_partition) GETSTRUCT(tuple);

		datum = SysCacheGetAttr(PARTITIONID, tuple,
									Anum_pg_partition_partlistvals, &isnull);
		if (!isnull)
		{
			deconstruct_array(DatumGetArrayTypeP(datum),
							  typinfo->typid[0],
							  typinfo->typlen[0],
							  typinfo->typbyval[0],
							  typinfo->typalign[0],
							  &result->listvalues, NULL, &result->listnvalues);

			ReleaseSysCache(tuple);
			partitions[i++] = result;
			continue;
		}

		/* Must be a range partition */
		datum = SysCacheGetAttr(PARTITIONID, tuple,
								Anum_pg_partition_partrangemaxs, &isnull);

		Assert(!isnull);

		/*
		 * Each element of rangemaxs (any) array is itself an array
		 * containing rangemax value for a given key column.
		 */
		deconstruct_array(DatumGetArrayTypeP(datum),
						  ANYARRAYOID, -1, false, 'd',
						  &rangemaxs, NULL, &rangenmaxs);
		/* Paranoia */
		Assert(rangenmaxs = result->partnatts);

		result->rangemaxs = (Datum *) palloc0(partnatts * sizeof(Datum));

		for (j = 0; j < result->partnatts; j++)
		{
			ArrayType  *arr = DatumGetArrayTypeP(rangemaxs[j]);
			Datum	   *datum;
			bool	   *nulls;
			int			dummy;

			deconstruct_array(arr,
							  typinfo->typid[j],
							  typinfo->typlen[j],
							  typinfo->typbyval[j],
							  typinfo->typalign[j],
							  &datum, &nulls, &dummy);

			/* Can't be null. */
			Assert(!nulls[0]);

			result->rangemaxs[j] = datum[0];
		}

		ReleaseSysCache(tuple);

		partitions[i++] = result;
	}

	free_key_type_info(typinfo);

	return partitions;
}

/* qsort comparison function */
static int
oid_cmp(const void *p1, const void *p2)
{
	Oid			v1 = *((const Oid *) p1);
	Oid			v2 = *((const Oid *) p2);

	if (v1 < v2)
		return -1;
	if (v1 > v2)
		return 1;
	return 0;
}
