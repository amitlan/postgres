/*-------------------------------------------------------------------------
 *
 * pg_plan_advice.h
 *	  main header file for pg_plan_advice contrib module
 *
 * Copyright (c) 2016-2024, PostgreSQL Global Development Group
 *
 *	  contrib/pg_plan_advice/pg_plan_advice.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PLAN_ADVICE_H
#define PG_PLAN_ADVICE_H

#include "nodes/plannodes.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"

typedef struct pgpa_shared_state
{
	LWLock		lock;
	int			dsa_tranche;
	dsa_handle	area;
	dsa_pointer shared_collector;
} pgpa_shared_state;

/* GUC variables */
extern int	pg_plan_advice_local_collection_limit;
extern int	pg_plan_advice_shared_collection_limit;
extern char *pg_plan_advice_advice;

/* Function prototypes */
extern MemoryContext pg_plan_advice_get_mcxt(void);
extern pgpa_shared_state *pg_plan_advice_attach(void);
extern dsa_area *pg_plan_advice_dsa_area(void);

#endif
