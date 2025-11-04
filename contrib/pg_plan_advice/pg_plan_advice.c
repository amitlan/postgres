/*-------------------------------------------------------------------------
 *
 * pg_plan_advice.c
 *	  main entrypoints for generating and applying planner advice
 *
 * Copyright (c) 2016-2024, PostgreSQL Global Development Group
 *
 *	  contrib/pg_plan_advice/pg_plan_advice.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_plan_advice.h"
#include "pgpa_ast.h"
#include "pgpa_collector.h"
#include "pgpa_identifier.h"
#include "pgpa_output.h"
#include "pgpa_planner.h"
#include "pgpa_trove.h"
#include "pgpa_walker.h"

#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#include "funcapi.h"
#include "optimizer/planner.h"
#include "storage/dsm_registry.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

static pgpa_shared_state *pgpa_state = NULL;
static dsa_area *pgpa_dsa_area = NULL;

/* GUC variables */
char	   *pg_plan_advice_advice = NULL;
static bool pg_plan_advice_always_explain_supplied_advice = true;
int			pg_plan_advice_local_collection_limit = 0;
int			pg_plan_advice_shared_collection_limit = 0;

/* Saved hook value */
static explain_per_plan_hook_type prev_explain_per_plan = NULL;

/* Other file-level globals */
static int	es_extension_id;
static MemoryContext pgpa_memory_context = NULL;

static void pg_plan_advice_explain_option_handler(ExplainState *es,
												  DefElem *opt,
												  ParseState *pstate);
static void pg_plan_advice_explain_per_plan_hook(PlannedStmt *plannedstmt,
												 IntoClause *into,
												 ExplainState *es,
												 const char *queryString,
												 ParamListInfo params,
												 QueryEnvironment *queryEnv);
static bool pg_plan_advice_advice_check_hook(char **newval, void **extra,
											 GucSource source);
static DefElem *find_defelem_by_defname(List *deflist, char *defname);

/*
 * Initialize this module.
 */
void
_PG_init(void)
{
	DefineCustomStringVariable("pg_plan_advice.advice",
							   "advice to apply during query planning",
							   NULL,
							   &pg_plan_advice_advice,
							   NULL,
							   PGC_USERSET,
							   0,
							   pg_plan_advice_advice_check_hook,
							   NULL,
							   NULL);

	DefineCustomBoolVariable("pg_plan_advice.always_explain_supplied_advice",
							 "EXPLAIN output includes supplied advice even without EXPLAIN (PLAN_ADVICE)",
							 NULL,
							 &pg_plan_advice_always_explain_supplied_advice,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_plan_advice.local_collection_limit",
							"# of advice entries to retain in per-backend memory",
							NULL,
							&pg_plan_advice_local_collection_limit,
							0,
							0, INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_plan_advice.shared_collection_limit",
							"# of advice entries to retain in shared memory",
							NULL,
							&pg_plan_advice_shared_collection_limit,
							0,
							0, INT_MAX,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	MarkGUCPrefixReserved("pg_plan_advice");

	/* Get an ID that we can use to cache data in an ExplainState. */
	es_extension_id = GetExplainExtensionId("pg_plan_advice");

	/* Register the new EXPLAIN options implemented by this module. */
	RegisterExtensionExplainOption("plan_advice",
								   pg_plan_advice_explain_option_handler);

	/* Install hooks */
	pgpa_planner_install_hooks();
	prev_explain_per_plan = explain_per_plan_hook;
	explain_per_plan_hook = pg_plan_advice_explain_per_plan_hook;
}

/*
 * Initialize shared state when first created.
 */
static void
pgpa_init_shared_state(void *ptr)
{
	pgpa_shared_state *state = (pgpa_shared_state *) ptr;

	LWLockInitialize(&state->lock, LWLockNewTrancheId("pg_plan_advice_lock"));
	state->dsa_tranche = LWLockNewTrancheId("pg_plan_advice_dsa");
	state->area = DSA_HANDLE_INVALID;
	state->shared_collector = InvalidDsaPointer;
}

/*
 * Return a pointer to a memory context where long-lived data managed by this
 * module can be stored.
 */
MemoryContext
pg_plan_advice_get_mcxt(void)
{
	if (pgpa_memory_context == NULL)
		pgpa_memory_context = AllocSetContextCreate(TopMemoryContext,
													"pg_plan_advice",
													ALLOCSET_DEFAULT_SIZES);

	return pgpa_memory_context;
}

/*
 * Get a pointer to our shared state.
 *
 * If no shared state exists, create and initialize it. If it does exist but
 * this backend has not yet accessed it, attach to it. Otherwise, just return
 * our cached pointer.
 *
 * Along the way, make sure the relevant LWLock tranches are registered.
 */
pgpa_shared_state *
pg_plan_advice_attach(void)
{
	if (pgpa_state == NULL)
	{
		bool		found;

		pgpa_state =
			GetNamedDSMSegment("pg_plan_advice", sizeof(pgpa_shared_state),
							   pgpa_init_shared_state, &found);
	}

	return pgpa_state;
}

/*
 * Return a pointer to pg_plan_advice's DSA area, creating it if needed.
 */
dsa_area *
pg_plan_advice_dsa_area(void)
{
	if (pgpa_dsa_area == NULL)
	{
		pgpa_shared_state *state = pg_plan_advice_attach();
		dsa_handle	area_handle;
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(pg_plan_advice_get_mcxt());

		LWLockAcquire(&state->lock, LW_EXCLUSIVE);
		area_handle = state->area;
		if (area_handle == DSA_HANDLE_INVALID)
		{
			pgpa_dsa_area = dsa_create(state->dsa_tranche);
			dsa_pin(pgpa_dsa_area);
			state->area = dsa_get_handle(pgpa_dsa_area);
			LWLockRelease(&state->lock);
		}
		else
		{
			LWLockRelease(&state->lock);
			pgpa_dsa_area = dsa_attach(area_handle);
		}

		dsa_pin_mapping(pgpa_dsa_area);

		MemoryContextSwitchTo(oldcontext);
	}

	return pgpa_dsa_area;
}

/*
 * Handler for EXPLAIN (PLAN_ADVICE).
 */
static void
pg_plan_advice_explain_option_handler(ExplainState *es, DefElem *opt,
									  ParseState *pstate)
{
	bool	   *plan_advice;

	plan_advice = GetExplainExtensionState(es, es_extension_id);

	if (plan_advice == NULL)
	{
		plan_advice = palloc0_object(bool);
		SetExplainExtensionState(es, es_extension_id, plan_advice);
	}

	*plan_advice = defGetBoolean(opt);
}

/*
 * Display a string that is likely to consist of multiple lines in EXPLAIN
 * output.
 */
static void
pg_plan_advice_explain_text_multiline(ExplainState *es, char *qlabel,
									  char *value)
{
	char	   *s;

	/* For non-text formats, it's best not to add any special handling. */
	if (es->format != EXPLAIN_FORMAT_TEXT)
	{
		ExplainPropertyText(qlabel, value, es);
		return;
	}

	/* In text format, if there is no data, display nothing. */
	if (*qlabel == '\0')
		return;

	/*
	 * It looks nicest to indent each line of the advice separately, beginning
	 * on the line below the label.
	 */
	ExplainIndentText(es);
	appendStringInfo(es->str, "%s:\n", qlabel);
	es->indent++;
	while ((s = strchr(value, '\n')) != NULL)
	{
		ExplainIndentText(es);
		appendBinaryStringInfo(es->str, value, (s - value) + 1);
		value = s + 1;
	}

	/* Don't interpret a terminal newline as a request for an empty line. */
	if (*value != '\0')
	{
		ExplainIndentText(es);
		appendStringInfo(es->str, "%s\n", value);
	}

	es->indent--;
}

/*
 * Add advice feedback to the EXPLAIN output.
 */
static void
pg_plan_advice_explain_feedback(ExplainState *es, List *feedback)
{
	StringInfoData buf;

	initStringInfo(&buf);
	foreach_node(DefElem, item, feedback)
	{
		int			flags = defGetInt32(item);

		appendStringInfo(&buf, "%s /* ", item->defname);
		if ((flags & PGPA_TE_MATCH_FULL) != 0)
		{
			Assert((flags & PGPA_TE_MATCH_PARTIAL) != 0);
			appendStringInfo(&buf, "matched");
		}
		else if ((flags & PGPA_TE_MATCH_PARTIAL) != 0)
			appendStringInfo(&buf, "partially matched");
		else
			appendStringInfo(&buf, "not matched");
		if ((flags & PGPA_TE_INAPPLICABLE) != 0)
			appendStringInfo(&buf, ", inapplicable");
		if ((flags & PGPA_TE_CONFLICTING) != 0)
			appendStringInfo(&buf, ", conflicting");
		if ((flags & PGPA_TE_FAILED) != 0)
			appendStringInfo(&buf, ", failed");
		appendStringInfo(&buf, " */\n");
	}

	pg_plan_advice_explain_text_multiline(es, "Supplied Plan Advice",
										  buf.data);
}

/*
 * Add relevant details, if any, to the EXPLAIN output for a single plan.
 */
static void
pg_plan_advice_explain_per_plan_hook(PlannedStmt *plannedstmt,
									 IntoClause *into,
									 ExplainState *es,
									 const char *queryString,
									 ParamListInfo params,
									 QueryEnvironment *queryEnv)
{
	bool	   *plan_advice = GetExplainExtensionState(es, es_extension_id);
	DefElem    *pgpa_item;
	List	   *pgpa_list;

	if (prev_explain_per_plan)
		prev_explain_per_plan(plannedstmt, into, es, queryString, params,
							  queryEnv);

	/* Find any data pgpa_planner_shutdown stashed in the PlannedStmt. */
	pgpa_item = find_defelem_by_defname(plannedstmt->extension_state,
										"pg_plan_advice");
	pgpa_list = pgpa_item == NULL ? NULL : (List *) pgpa_item->arg;

	/*
	 * By default, if there is a record of attempting to apply advice during
	 * query planning, we always output that information, but the user can set
	 * pg_plan_advice.always_explain_supplied_advice = false to suppress that
	 * behavior. If they do, we'll only display it when the PLAN_ADVICE option
	 * was specified and not set to false.
	 *
	 * NB: If we're explaining a query planned beforehand -- i.e. a prepared
	 * statement -- the application of query advice may not have been
	 * recorded, and therefore this won't be able to show anything.
	 */
	if (pgpa_list != NULL && (pg_plan_advice_always_explain_supplied_advice ||
							  (plan_advice != NULL && *plan_advice)))
	{
		DefElem    *feedback;

		feedback = find_defelem_by_defname(pgpa_list, "feedback");
		if (feedback != NULL)
			pg_plan_advice_explain_feedback(es, (List *) feedback->arg);
	}

	/*
	 * If the PLAN_ADVICE option was specified -- and not sent to FALSE --
	 * show generated advice.
	 */
	if (plan_advice != NULL && *plan_advice)
	{
		DefElem    *advice_string_item;
		char	   *advice_string;

		advice_string_item =
			find_defelem_by_defname(pgpa_list, "advice_string");
		if (advice_string_item != NULL)
		{
			/* Advice has already been generated; we can reuse it. */
			advice_string = strVal(advice_string_item->arg);
		}
		else
		{
			pgpa_plan_walker_context walker;
			StringInfoData buf;
			pgpa_identifier *rt_identifiers;

			/* Advice not yet generated; do that now. */
			pgpa_plan_walker(&walker, plannedstmt);
			rt_identifiers =
				pgpa_create_identifiers_for_planned_stmt(plannedstmt);
			initStringInfo(&buf);
			pgpa_output_advice(&buf, &walker, rt_identifiers);
			advice_string = buf.data;
		}

		if (advice_string[0] != '\0')
			pg_plan_advice_explain_text_multiline(es, "Generated Plan Advice",
												  advice_string);
	}
}

/*
 * Check hook for pg_plan_advice.advice
 */
static bool
pg_plan_advice_advice_check_hook(char **newval, void **extra, GucSource source)
{
	MemoryContext oldcontext;
	MemoryContext tmpcontext;
	char	   *error;

	if (*newval == NULL)
		return true;

	tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "pg_plan_advice.advice",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(tmpcontext);

	/*
	 * It would be nice to save the parse tree that we construct here for
	 * eventual use when planning with this advice, but *extra can only point
	 * to a single guc_malloc'd chunk, and our parse tree involves an
	 * arbitrary number of memory allocations.
	 */
	(void) pgpa_parse(*newval, &error);

	if (error != NULL)
	{
		GUC_check_errdetail("Could not parse advice: %s", error);
		return false;
	}

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(tmpcontext);

	return true;
}

/*
 * Search a list of DefElem objects for a given defname.
 */
static DefElem *
find_defelem_by_defname(List *deflist, char *defname)
{
	foreach_node(DefElem, item, deflist)
	{
		if (strcmp(item->defname, defname) == 0)
			return item;
	}

	return NULL;
}
