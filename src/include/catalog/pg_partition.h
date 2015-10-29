/*-------------------------------------------------------------------------
 *
 * pg_partition.h
 *	  definition of the system "partition" relation (pg_partition)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 * $PostgreSQL: pgsql/src/include/catalog/pg_partition.h $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PARTITION_H
#define PG_PARTITION_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_partitioned_rel definition.  cpp turns this into
 *		typedef struct FormData_pg_partitioned_rel
 * ----------------
 */
#define PartitionRelationId 3309

CATALOG(pg_partition,3309) BKI_WITHOUT_OIDS
{
	Oid			partrelid;		/* partition oid */
	Oid			partparent;		/* parent oid */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	anyarray	partlistvals;	/* array of allowed values for the only
								 * partition column */
	anyarray	partrangemaxs;	/* array of rangemax values, one per
								 * key column */
#endif
} FormData_pg_partition;

/* ----------------
 *      Form_pg_partition corresponds to a pointer to a tuple with
 *      the format of pg_partition relation.
 * ----------------
 */
typedef FormData_pg_partition *Form_pg_partition;

/* ----------------
 *      compiler constants for pg_partition
 * ----------------
 */
#define Natts_pg_partition					4
#define Anum_pg_partition_partrelid			1
#define Anum_pg_partition_partparent		2
#define Anum_pg_partition_partlistvals		3
#define Anum_pg_partition_partrangemaxs		4

#endif   /* PG_PARTITION_H */
