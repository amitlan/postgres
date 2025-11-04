/*-------------------------------------------------------------------------
 *
 * pgpa_collector.h
 *	  collect advice into backend-local or shared memory
 *
 * Copyright (c) 2016-2025, PostgreSQL Global Development Group
 *
 *	  contrib/pg_plan_advice/pgpa_collector.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGPA_COLLECTOR_H
#define PGPA_COLLECTOR_H

extern void pgpa_collect_advice(uint64 queryId, const char *query_string,
								const char *advice_string);

#endif
