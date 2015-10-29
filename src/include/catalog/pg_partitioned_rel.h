/*-------------------------------------------------------------------------
 *
 * pg_partitioned_rel.h
 *	  definition of the system "partitioned" relation (pg_partitioned_rel)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 * $PostgreSQL: pgsql/src/include/catalog/pg_partitioned_rel.h $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PARTITIONED_REL_H
#define PG_PARTITIONED_REL_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_partitioned_rel definition.  cpp turns this into
 *		typedef struct FormData_pg_partitioned_rel
 * ----------------
 */
#define PartitionedRelRelationId 3308

CATALOG(pg_partitioned_rel,3308) BKI_WITHOUT_OIDS
{
	Oid				partrelid;		/* partitioned table oid */
	char			partstrat;		/* partitioning strategy */
	int16			partnatts;		/* number of partition columns */

	/* variable-length fields start here, but we allow direct access to indkey */
	int2vector		partkey;		/* attribute numbers of partition
									 * columns */

#ifdef CATALOG_VARLEN
	oidvector		partclass;		/* operator class to compare keys */
	pg_node_tree	partexprs;		/* expression trees for partition key members
									 * that are not simple column references; one
									 * for each zero entry in partkey[] */
#endif
} FormData_pg_partitioned_rel;

/* ----------------
 *      Form_pg_partitioned_rel corresponds to a pointer to a tuple with
 *      the format of pg_partitioned_rel relation.
 * ----------------
 */
typedef FormData_pg_partitioned_rel *Form_pg_partitioned_rel;

/* ----------------
 *      compiler constants for pg_partitioned_rel
 * ----------------
 */
#define Natts_pg_partitioned_rel				6
#define Anum_pg_partitioned_rel_partrelid		1
#define Anum_pg_partitioned_rel_partstrat		2
#define Anum_pg_partitioned_rel_partnatts		3
#define Anum_pg_partitioned_rel_partkey			4
#define Anum_pg_partitioned_rel_partclass		5
#define Anum_pg_partitioned_rel_partexprs		6

#endif   /* PG_PARTITIONED_REL_H */
