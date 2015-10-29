/*-------------------------------------------------------------------------
 *
 * partition.h
 *	  Header file for utility functions related to partitioning
 *
 * Copyright (c) 2007-2015, PostgreSQL Global Development Group
 *
 * src/include/utils/partition.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARTITION_H
#define PARTITION_H

extern bool list_partition_overlaps(Relation rel,
								Datum *listvalues,
								int listnvalues,
								Oid *overlapsWith);
extern bool range_partition_empty(Relation rel,
								  Datum *rangemaxs);
extern bool range_partition_overlaps(Relation rel,
									 Datum *rangemaxs);
extern Oid get_partition_for_tuple(Relation rel,
								PartitionKeyInfo *pkinfo,
								PartitionDesc *pdesc,
								TupleTableSlot *slot,
								EState *estate);
extern List *get_all_partitions(Relation rel);
extern List *get_partitions_for_clauses(Relation rel,
								int *nclauses,
								Datum **clauseval,
								Oid **clauseop);
#endif   /* PARTITION_H */
