/*-------------------------------------------------------------------------
 *
 * partitioning.h
 *	  prototypes for partitioning.c
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/partitioning.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARTITIONING_H
#define PARTITIONING_H

#include "nodes/primnodes.h"

extern List *get_rel_partitions(RelOptInfo *rel, RangeTblEntry *rte);

#endif   /* PARTITIONING_H */
