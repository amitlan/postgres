/*-------------------------------------------------------------------------
 *
 * pg_partition_fn.h
 *	  prototypes for functions in catalog/pg_partition.c
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_partition_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PARTITION_FN_H
#define PG_PARTITION_FN_H

#include "nodes/pg_list.h"
#include "utils/rel.h"

/* Bound info of a single partition */
typedef struct PartitionBoundInfo
{
	Oid		oid;			/* Partition relationID */
	int		partnatts;		/* qsort_arg's callback function uses this */
	int		listnvalues;	/* How many values does listvalues below hold */
	Datum  *listvalues;		/* List partition values */
	Datum  *rangemaxs;		/* Range partition max bound - one datum per
							   key column */
} PartitionBoundInfo;

extern PartitionBoundInfo **GetPartitionBounds(Relation rel, int *numparts);
extern void free_partitions(PartitionBoundInfo **p, int count);
extern Oid get_partition_parent(Oid relid);
extern List *find_partitions(Oid relid, LOCKMODE lockmode);

#endif   /* PG_PARTITION_FN_H */
