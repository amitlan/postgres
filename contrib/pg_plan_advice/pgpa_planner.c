/*-------------------------------------------------------------------------
 *
 * pgpa_planner.c
 *	  planner hooks
 *
 * Copyright (c) 2016-2024, PostgreSQL Global Development Group
 *
 *	  contrib/pg_plan_advice/pgpa_planner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_plan_advice.h"
#include "pgpa_collector.h"
#include "pgpa_identifier.h"
#include "pgpa_output.h"
#include "pgpa_planner.h"
#include "pgpa_trove.h"
#include "pgpa_walker.h"

#include "common/hashfn_unstable.h"
#include "nodes/makefuncs.h"
#include "optimizer/extendplan.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

#ifdef USE_ASSERT_CHECKING

/*
 * When assertions are enabled, we try generating relation identifiers during
 * planning, saving them in a hash table, and then cross-checking them against
 * the ones generated after planning is complete.
 */
typedef struct pgpa_ri_checker_key
{
	char	   *plan_name;
	Index		rti;
} pgpa_ri_checker_key;

typedef struct pgpa_ri_checker
{
	pgpa_ri_checker_key key;
	uint32		status;
	const char *rid_string;
} pgpa_ri_checker;

static uint32 pgpa_ri_checker_hash_key(pgpa_ri_checker_key key);

static inline bool
pgpa_ri_checker_compare_key(pgpa_ri_checker_key a, pgpa_ri_checker_key b)
{
	if (a.rti != b.rti)
		return false;
	if (a.plan_name == NULL)
		return (b.plan_name == NULL);
	if (b.plan_name == NULL)
		return false;
	return strcmp(a.plan_name, b.plan_name) == 0;
}

#define SH_PREFIX			pgpa_ri_check
#define SH_ELEMENT_TYPE		pgpa_ri_checker
#define SH_KEY_TYPE			pgpa_ri_checker_key
#define SH_KEY				key
#define SH_HASH_KEY(tb, key)	pgpa_ri_checker_hash_key(key)
#define	SH_EQUAL(tb, a, b)	pgpa_ri_checker_compare_key(a, b)
#define SH_SCOPE			static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

#endif

typedef struct pgpa_planner_state
{
	ExplainState *explain_state;
	pgpa_trove *trove;
	MemoryContext trove_cxt;

#ifdef USE_ASSERT_CHECKING
	pgpa_ri_check_hash *ri_check_hash;
#endif
} pgpa_planner_state;

typedef struct pgpa_join_state
{
	/* Most-recently-considered outer rel. */
	RelOptInfo *outerrel;

	/* Most-recently-considered inner rel. */
	RelOptInfo *innerrel;

	/*
	 * Array of relation identifiers for all members of this joinrel, with
	 * outerrel idenifiers before innerrel identifiers.
	 */
	pgpa_identifier *rids;

	/* Number of outer rel identifiers. */
	int			outer_count;

	/* Number of inner rel identifiers. */
	int			inner_count;

	/*
	 * Trove lookup results.
	 *
	 * join_entries and rel_entries are arrays of entries, and join_indexes
	 * and rel_indexes are the integer offsets within those arrays of entries
	 * potentially relevant to us. The "join" fields correspond to a lookup
	 * using PGPA_TROVE_LOOKUP_JOIN and the "rel" fields to a lookup using
	 * PGPA_TROVE_LOOKUP_REL.
	 */
	pgpa_trove_entry *join_entries;
	Bitmapset  *join_indexes;
	pgpa_trove_entry *rel_entries;
	Bitmapset  *rel_indexes;
} pgpa_join_state;

/* Saved hook values */
static get_relation_info_hook_type prev_get_relation_info = NULL;
static join_path_setup_hook_type prev_join_path_setup = NULL;
static joinrel_setup_hook_type prev_joinrel_setup = NULL;
static planner_setup_hook_type prev_planner_setup = NULL;
static planner_shutdown_hook_type prev_planner_shutdown = NULL;

/* Other global variabes */
static int	planner_extension_id = -1;

/* Function prototypes. */
static void pgpa_get_relation_info(PlannerInfo *root,
								   Oid relationObjectId,
								   bool inhparent,
								   RelOptInfo *rel);
static void pgpa_joinrel_setup(PlannerInfo *root,
							   RelOptInfo *joinrel,
							   RelOptInfo *outerrel,
							   RelOptInfo *innerrel,
							   SpecialJoinInfo *sjinfo,
							   List *restrictlist);
static void pgpa_join_path_setup(PlannerInfo *root,
								 RelOptInfo *joinrel,
								 RelOptInfo *outerrel,
								 RelOptInfo *innerrel,
								 JoinType jointype,
								 JoinPathExtraData *extra);
static void pgpa_planner_setup(PlannerGlobal *glob, Query *parse,
							   const char *query_string,
							   double *tuple_fraction,
							   ExplainState *es);
static void pgpa_planner_shutdown(PlannerGlobal *glob, Query *parse,
								  const char *query_string, PlannedStmt *pstmt);
static void pgpa_planner_apply_joinrel_advice(uint64 *pgs_mask_p,
											  char *plan_name,
											  pgpa_join_state *pjs);
static void pgpa_planner_apply_join_path_advice(JoinType jointype,
												uint64 *pgs_mask_p,
												char *plan_name,
												pgpa_join_state *pjs);
static void pgpa_planner_apply_scan_advice(RelOptInfo *rel,
										   pgpa_trove_entry *scan_entries,
										   Bitmapset *scan_indexes,
										   pgpa_trove_entry *rel_entries,
										   Bitmapset *rel_indexes);
static uint64 pgpa_join_strategy_mask_from_advice_tag(pgpa_advice_tag_type tag);
static bool pgpa_join_order_permits_join(int outer_count, int inner_count,
										 pgpa_identifier *rids,
										 pgpa_trove_entry *entry);
static bool pgpa_join_method_permits_join(int outer_count, int inner_count,
										  pgpa_identifier *rids,
										  pgpa_trove_entry *entry,
										  bool *restrict_method);
static bool pgpa_opaque_join_permits_join(int outer_count, int inner_count,
										  pgpa_identifier *rids,
										  pgpa_trove_entry *entry,
										  bool *restrict_method);

static List *pgpa_planner_append_feedback(List *list, pgpa_trove *trove,
										  pgpa_trove_lookup_type type,
										  pgpa_identifier *rt_identifiers,
										  pgpa_plan_walker_context *walker);

static inline void pgpa_ri_checker_save(pgpa_planner_state *pps,
										PlannerInfo *root,
										RelOptInfo *rel);
static void pgpa_ri_checker_validate(pgpa_planner_state *pps,
									 PlannedStmt *pstmt);

/*
 * Install planner-related hooks.
 */
void
pgpa_planner_install_hooks(void)
{
	planner_extension_id = GetPlannerExtensionId("pg_plan_advice");
	prev_get_relation_info = get_relation_info_hook;
	get_relation_info_hook = pgpa_get_relation_info;
	prev_joinrel_setup = joinrel_setup_hook;
	joinrel_setup_hook = pgpa_joinrel_setup;
	prev_join_path_setup = join_path_setup_hook;
	join_path_setup_hook = pgpa_join_path_setup;
	prev_planner_setup = planner_setup_hook;
	planner_setup_hook = pgpa_planner_setup;
	prev_planner_shutdown = planner_shutdown_hook;
	planner_shutdown_hook = pgpa_planner_shutdown;
}

/*
 * Hook function for get_relation_info().
 *
 * We can apply scan advice at this opint, and we also usee this as an
 * opportunity to do range-table identifier cross-checking in assert-enabled
 * builds.
 *
 * XXX: We currently emit useless advice like NO_GATHER("*RESULT*") for trivial
 * queries. The advice is useless because get_relation_info isn't called for
 * non-relation RTEs. We should either suppress the advice in such cases, or
 * add a hook that can apply it.
 */
static void
pgpa_get_relation_info(PlannerInfo *root, Oid relationObjectId,
					   bool inhparent, RelOptInfo *rel)
{
	pgpa_planner_state *pps;

	/* Fetch our private state, set up by pgpa_planner_setup(). */
	pps = GetPlannerGlobalExtensionState(root->glob, planner_extension_id);

	/* Save details needed for range table identifier cross-checking. */
	if (pps != NULL)
		pgpa_ri_checker_save(pps, root, rel);

	/* If query advice was provided, search for relevant entries. */
	if (pps != NULL && pps->trove != NULL)
	{
		pgpa_identifier rid;
		pgpa_trove_result tresult_scan;
		pgpa_trove_result tresult_rel;

		/* Search for scan advice and general rel advice. */
		pgpa_compute_identifier_by_rti(root, rel->relid, &rid);
		pgpa_trove_lookup(pps->trove, PGPA_TROVE_LOOKUP_SCAN, 1, &rid,
						  &tresult_scan);
		pgpa_trove_lookup(pps->trove, PGPA_TROVE_LOOKUP_REL, 1, &rid,
						  &tresult_rel);

		/* If relevant entries were found, apply them. */
		if (tresult_scan.indexes != NULL || tresult_rel.indexes != NULL)
			pgpa_planner_apply_scan_advice(rel,
										   tresult_scan.entries,
										   tresult_scan.indexes,
										   tresult_rel.entries,
										   tresult_rel.indexes);
	}

	/* Pass call to previous hook. */
	if (prev_get_relation_info)
		(*prev_get_relation_info) (root, relationObjectId, inhparent, rel);
}

/*
 * Search for advice pertaining to a proposed join.
 */
static pgpa_join_state *
pgpa_get_join_state(PlannerInfo *root, RelOptInfo *joinrel,
					RelOptInfo *outerrel, RelOptInfo *innerrel)
{
	pgpa_planner_state *pps;
	pgpa_join_state *pjs;
	bool		new_pjs = false;

	/* Fetch our private state, set up by pgpa_planner_setup(). */
	pps = GetPlannerGlobalExtensionState(root->glob, planner_extension_id);
	if (pps == NULL || pps->trove == NULL)
	{
		/* No advice applies to this query, hence none to this joinrel. */
		return NULL;
	}

	/*
	 * See whether we've previously associated a pgpa_join_state with this
	 * joinrel. If we have not, we need to try to construct one. If we have,
	 * then there are two cases: (a) if innerrel and outerrel are unchanged,
	 * we can simply use it, and (b) if they have changed, we need to rejigger
	 * the array of identifiers but can still skip the trove lookup.
	 */
	pjs = GetRelOptInfoExtensionState(joinrel, planner_extension_id);
	if (pjs != NULL)
	{
		if (pjs->join_indexes == NULL && pjs->rel_indexes == NULL)
		{
			/*
			 * If there's no potentially relevant advice, then the presence of
			 * this pgpa_join_state acts like a negative cache entry: it tells
			 * us not to bother searching the trove for advice, because we
			 * will not find any.
			 */
			return NULL;
		}

		if (pjs->outerrel == outerrel && pjs->innerrel == innerrel)
		{
			/* No updates required, so just return. */
			/* XXX. Does this need to do something different under GEQO? */
			return pjs;
		}
	}

	/*
	 * If there's no pgpa_join_state yet, we need to allocate one. Trove keys
	 * will not get built for RTE_JOIN RTEs, so the array may end up being
	 * larger than needed. It's not worth trying to compute a perfectly
	 * accurate count here.
	 */
	if (pjs == NULL)
	{
		int			pessimistic_count = bms_num_members(joinrel->relids);

		pjs = palloc0_object(pgpa_join_state);
		pjs->rids = palloc_array(pgpa_identifier, pessimistic_count);
		new_pjs = true;
	}

	/*
	 * Either we just allocated a new pgpa_join_state, or the existing one
	 * needs reconfiguring for a new innerrel and outerrel. The required array
	 * size can't change, so we can overwrite the existing one.
	 */
	pjs->outerrel = outerrel;
	pjs->innerrel = innerrel;
	pjs->outer_count =
		pgpa_compute_identifiers_by_relids(root, outerrel->relids, pjs->rids);
	pjs->inner_count =
		pgpa_compute_identifiers_by_relids(root, innerrel->relids,
										   pjs->rids + pjs->outer_count);

	/*
	 * If we allocated a new pgpa_join_state, search our trove of advice for
	 * relevant entries. The trove lookup will return the same results for
	 * every outerrel/innerrel combination, so we don't need to repeat that
	 * work every time.
	 */
	if (new_pjs)
	{
		pgpa_trove_result tresult;

		/* Find join entries. */
		pgpa_trove_lookup(pps->trove, PGPA_TROVE_LOOKUP_JOIN,
						  pjs->outer_count + pjs->inner_count,
						  pjs->rids, &tresult);
		pjs->join_entries = tresult.entries;
		pjs->join_indexes = tresult.indexes;

		/* Find rel entries. */
		pgpa_trove_lookup(pps->trove, PGPA_TROVE_LOOKUP_REL,
						  pjs->outer_count + pjs->inner_count,
						  pjs->rids, &tresult);
		pjs->rel_entries = tresult.entries;
		pjs->rel_indexes = tresult.indexes;

		/* Now that the new pgpa_join_state is fully valid, save a pointer. */
		SetRelOptInfoExtensionState(joinrel, planner_extension_id, pjs);

		/*
		 * If there was no relevant advice found, just return NULL. This
		 * pgpa_join_state will stick around as a sort of negative cache
		 * entry, so that future calls for this same joinrel quickly return
		 * NULL.
		 */
		if (pjs->join_indexes == NULL && pjs->rel_indexes == NULL)
			return NULL;
	}

	return pjs;
}

/*
 * Enforce any provided advice that is relevant to any method of implementing
 * this join.
 *
 * Although we're passed the outerrel and innerrel here, those are just
 * whatever values happened to prompt the creation of this joinrel; they
 * shouldn't really influence our choice of what advice to apply.
 */
static void
pgpa_joinrel_setup(PlannerInfo *root, RelOptInfo *joinrel,
				   RelOptInfo *outerrel, RelOptInfo *innerrel,
				   SpecialJoinInfo *sjinfo, List *restrictlist)
{
	pgpa_join_state *pjs;

	Assert(bms_membership(joinrel->relids) == BMS_MULTIPLE);

	/* Get our private state information for this join. */
	pjs = pgpa_get_join_state(root, joinrel, outerrel, innerrel);

	/* If there is relevant advice, call a helper function to apply it. */
	if (pjs != NULL)
		pgpa_planner_apply_joinrel_advice(&joinrel->pgs_mask,
										  root->plan_name,
										  pjs);

	/* Pass call to previous hook. */
	if (prev_joinrel_setup)
		(*prev_joinrel_setup) (root, joinrel, outerrel, innerrel,
							   sjinfo, restrictlist);
}

/*
 * Enforce any provided advice that is relevant to this particular method of
 * implementing this particular join.
 */
static void
pgpa_join_path_setup(PlannerInfo *root, RelOptInfo *joinrel,
					 RelOptInfo *outerrel, RelOptInfo *innerrel,
					 JoinType jointype, JoinPathExtraData *extra)
{
	pgpa_join_state *pjs;

	Assert(bms_membership(joinrel->relids) == BMS_MULTIPLE);

	/* Get our private state information for this join. */
	pjs = pgpa_get_join_state(root, joinrel, outerrel, innerrel);

	/* If there is relevant advice, call a helper function to apply it. */
	if (pjs != NULL)
		pgpa_planner_apply_join_path_advice(jointype,
											&extra->pgs_mask,
											root->plan_name,
											pjs);

	/* Pass call to previous hook. */
	if (prev_join_path_setup)
		(*prev_join_path_setup) (root, joinrel, outerrel, innerrel,
								 jointype, extra);
}

/*
 * Prepare advice for use by a query.
 */
static void
pgpa_planner_setup(PlannerGlobal *glob, Query *parse, const char *query_string,
				   double *tuple_fraction, ExplainState *es)
{
	pgpa_trove *trove = NULL;
	pgpa_planner_state *pps;
	bool		needs_pps = false;

	/*
	 * If any advice was provided, build a trove of advice for use during
	 * planning.
	 */
	if (pg_plan_advice_advice != NULL && pg_plan_advice_advice[0] != '\0')
	{
		List	   *advice_items;
		char	   *error;

		/*
		 * Parsing shouldn't fail here, because we must have previously parsed
		 * successfully in pg_plan_advice_advice_check_hook, but if it does,
		 * emit a warning.
		 */
		advice_items = pgpa_parse(pg_plan_advice_advice, &error);
		if (error)
			elog(WARNING, "could not parse advice: %s", error);

		/*
		 * It's possible that the advice string was non-empty but contained no
		 * actual advice, e.g. it was all whitespace.
		 */
		if (advice_items != NIL)
		{
			trove = pgpa_build_trove(advice_items);
			needs_pps = true;
		}
	}

#ifdef USE_ASSERT_CHECKING

	/*
	 * If asserts are enabled, always build a private state object for
	 * cross-checks.
	 */
	needs_pps = true;
#endif

	/* Initialize and store private state, if required. */
	if (needs_pps)
	{
		pps = palloc0_object(pgpa_planner_state);
		pps->explain_state = es;
		pps->trove = trove;
#ifdef USE_ASSERT_CHECKING
		pps->ri_check_hash =
			pgpa_ri_check_create(CurrentMemoryContext, 1024, NULL);
#endif
		SetPlannerGlobalExtensionState(glob, planner_extension_id, pps);
	}
}

/*
 * Carry out whatever work we want to do after planning is complete.
 */
static void
pgpa_planner_shutdown(PlannerGlobal *glob, Query *parse,
					  const char *query_string, PlannedStmt *pstmt)
{
	pgpa_planner_state *pps;
	pgpa_trove *trove = NULL;
	ExplainState *es = NULL;
	pgpa_plan_walker_context walker = {0};	/* placate compiler */
	bool		do_advice_feedback;
	bool		do_collect_advice;
	List	   *pgpa_items = NIL;
	pgpa_identifier *rt_identifiers = NULL;

	/* Fetch our private state, set up by pgpa_planner_setup(). */
	pps = GetPlannerGlobalExtensionState(glob, planner_extension_id);
	if (pps != NULL)
	{
		trove = pps->trove;
		es = pps->explain_state;
	}

	/* If at least one collector is enabled, generate advice. */
	do_collect_advice = (pg_plan_advice_local_collection_limit > 0 ||
						 pg_plan_advice_shared_collection_limit > 0);

	/* If we applied advice, generate feedback. */
	do_advice_feedback = (trove != NULL && es != NULL);

	/* If either of the above apply, analyze the resulting PlannedStmt. */
	if (do_collect_advice || do_advice_feedback)
	{
		pgpa_plan_walker(&walker, pstmt);
		rt_identifiers = pgpa_create_identifiers_for_planned_stmt(pstmt);
	}

	/*
	 * If advice collection is enabled, put the advice in string form and send
	 * it to the collector.
	 */
	if (do_collect_advice)
	{
		char	   *advice_string;
		StringInfoData buf;

		/* Generate a textual advice string. */
		initStringInfo(&buf);
		pgpa_output_advice(&buf, &walker, rt_identifiers);
		advice_string = buf.data;

		/* If the advice string is empty, don't bother collecting it. */
		if (advice_string[0] != '\0')
			pgpa_collect_advice(pstmt->queryId, query_string, advice_string);

		/*
		 * If we've gone to the trouble of generating an advice string, and if
		 * we're inside EXPLAIN, save the string so we don't need to
		 * regenerate it.
		 */
		if (es != NULL)
			pgpa_items = lappend(pgpa_items,
								 makeDefElem("advice_string",
											 (Node *) makeString(advice_string),
											 -1));
	}

	/*
	 * If we are planning within EXPLAIN, make arrangements to allow EXPLAIN
	 * to tell the user what has happened with the provided advice.
	 *
	 * NB: If EXPLAIN is used on a prepared is a prepared statement, planning
	 * will have already happened happened without recording these details. We
	 * could consider adding a GUC to cater to that scenario; or we could do
	 * this work all the time, but that seems like too much overhead.
	 */
	if (do_advice_feedback)
	{
		List	   *feedback = NIL;

		/*
		 * Inject a Node-tree representation of all the trove-entry flags into
		 * the PlannedStmt.
		 */
		feedback = pgpa_planner_append_feedback(feedback,
												trove,
												PGPA_TROVE_LOOKUP_SCAN,
												rt_identifiers, &walker);
		feedback = pgpa_planner_append_feedback(feedback,
												trove,
												PGPA_TROVE_LOOKUP_JOIN,
												rt_identifiers, &walker);
		feedback = pgpa_planner_append_feedback(feedback,
												trove,
												PGPA_TROVE_LOOKUP_REL,
												rt_identifiers, &walker);

		pgpa_items = lappend(pgpa_items, makeDefElem("feedback",
													 (Node *) feedback,
													 -1));
	}

	/* Push whatever data we're saving into the PlannedStmt. */
	if (pgpa_items != NIL)
		pstmt->extension_state =
			lappend(pstmt->extension_state,
					makeDefElem("pg_plan_advice", (Node *) pgpa_items, -1));

	/*
	 * If assertions are enabled, cross-check the generated range table
	 * identifiers.
	 */
	if (pps != NULL)
		pgpa_ri_checker_validate(pps, pstmt);
}

/*
 * Enforce overall restrictions on a join relation that apply uniformly
 * regardless of the choice of inner and outer rel.
 */
static void
pgpa_planner_apply_joinrel_advice(uint64 *pgs_mask_p, char *plan_name,
								  pgpa_join_state *pjs)
{
	int			i = -1;
	int			flags;
	bool		gather_conflict = false;
	uint64		gather_mask = 0;
	Bitmapset  *gather_partial_match = NULL;
	Bitmapset  *gather_full_match = NULL;
	bool		partitionwise_conflict = false;
	int			partitionwise_outcome = 0;
	Bitmapset  *partitionwise_partial_match = NULL;
	Bitmapset  *partitionwise_full_match = NULL;

	/* Iterate over all possibly-relevant advice. */
	while ((i = bms_next_member(pjs->rel_indexes, i)) >= 0)
	{
		pgpa_trove_entry *entry = &pjs->rel_entries[i];
		pgpa_itm_type itm;
		bool		full_match = false;
		uint64		my_gather_mask = 0;
		int			my_partitionwise_outcome = 0;	/* >0 yes, <0 no */

		/*
		 * For GATHER and GATHER_MERGE, if the specified relations exactly
		 * match this joinrel, do whatever the advice says; otherwise, don't
		 * allow Gather or Gather Merge at this level. For NO_GATHER, there
		 * must be a single target relation which must be included in this
		 * joinrel, so just don't allow Gather or Gather Merge here, full
		 * stop.
		 */
		if (entry->tag == PGPA_TAG_NO_GATHER)
		{
			my_gather_mask = PGS_CONSIDER_NONPARTIAL;
			full_match = true;
		}
		else
		{
			int			total_count;

			total_count = pjs->outer_count + pjs->inner_count;
			itm = pgpa_identifiers_match_target(total_count, pjs->rids,
												entry->target);
			Assert(itm != PGPA_ITM_DISJOINT);

			if (itm == PGPA_ITM_EQUAL)
			{
				full_match = true;
				if (entry->tag == PGPA_TAG_PARTITIONWISE)
					my_partitionwise_outcome = 1;
				else if (entry->tag == PGPA_TAG_GATHER)
					my_gather_mask = PGS_GATHER;
				else if (entry->tag == PGPA_TAG_GATHER_MERGE)
					my_gather_mask = PGS_GATHER_MERGE;
				else
					elog(ERROR, "unexpected advice tag: %d",
						 (int) entry->tag);
			}
			else
			{
				if (entry->tag == PGPA_TAG_PARTITIONWISE)
				{
					my_partitionwise_outcome = -1;
					my_gather_mask = PGS_CONSIDER_NONPARTIAL;
				}
				else if (entry->tag == PGPA_TAG_GATHER ||
						 entry->tag == PGPA_TAG_GATHER_MERGE)
				{
					my_partitionwise_outcome = -1;
					my_gather_mask = PGS_CONSIDER_NONPARTIAL;
				}
				else
					elog(ERROR, "unexpected advice tag: %d",
						 (int) entry->tag);
			}
		}

		/*
		 * If we set my_gather_mask up above, then we (1) make a note if the
		 * advice conflicted, (2) remember the mask value, and (3) remember
		 * whether this was a full or partial match.
		 */
		if (my_gather_mask != 0)
		{
			if (gather_mask != 0 && gather_mask != my_gather_mask)
				gather_conflict = true;
			gather_mask = my_gather_mask;
			if (full_match)
				gather_full_match = bms_add_member(gather_full_match, i);
			else
				gather_partial_match = bms_add_member(gather_partial_match, i);
		}

		/*
		 * Likewise, if we set my_partitionwise_outcome up above, then we (1)
		 * make a note if the advice conflicted, (2) remember what the desired
		 * outcome was, and (3) remember whether this was a full or partial
		 * match.
		 */
		if (my_partitionwise_outcome != 0)
		{
			if (partitionwise_outcome != 0 &&
				partitionwise_outcome != my_partitionwise_outcome)
				partitionwise_conflict = true;
			partitionwise_outcome = my_partitionwise_outcome;
			if (full_match)
				partitionwise_full_match =
					bms_add_member(partitionwise_full_match, i);
			else
				partitionwise_partial_match =
					bms_add_member(partitionwise_partial_match, i);
		}
	}

	/*
	 * Mark every Gather-related piece of advice as partially matched, and if
	 * the set of targets exactly matched this relation, fully matched. If
	 * there was a conflict, mark them all as conflicting.
	 */
	flags = PGPA_TE_MATCH_PARTIAL;
	if (gather_conflict)
		flags |= PGPA_TE_CONFLICTING;
	pgpa_trove_set_flags(pjs->rel_entries, gather_partial_match, flags);
	flags |= PGPA_TE_MATCH_FULL;
	pgpa_trove_set_flags(pjs->rel_entries, gather_full_match, flags);

	/* Likewise for partitionwise advice. */
	flags = PGPA_TE_MATCH_PARTIAL;
	if (partitionwise_conflict)
		flags |= PGPA_TE_CONFLICTING;
	pgpa_trove_set_flags(pjs->rel_entries, partitionwise_partial_match, flags);
	flags |= PGPA_TE_MATCH_FULL;
	pgpa_trove_set_flags(pjs->rel_entries, partitionwise_full_match, flags);

	/* If there is a non-conflicting gather specification, enforce it. */
	if (gather_mask != 0 && !gather_conflict)
	{
		*pgs_mask_p &=
			~(PGS_GATHER | PGS_GATHER_MERGE | PGS_CONSIDER_NONPARTIAL);
		*pgs_mask_p |= gather_mask;
	}

	/*
	 * If there is a non-conflicting partitionwise specification, enforce.
	 *
	 * To force a partitionwise join, we disable all the ordinary means of
	 * performing a join, and instead only Append and MergeAppend paths here.
	 * To prevent one, we just disable Append and MergeAppend.  Note that we
	 * must not unset PGS_CONSIDER_PARTITIONWISE even when we don't want a
	 * partitionwise join here, because we might want one at a higher level
	 * that is constructing using paths from this level.
	 */
	if (partitionwise_outcome != 0 && !partitionwise_conflict)
	{
		if (partitionwise_outcome > 0)
			*pgs_mask_p = (*pgs_mask_p & ~PGS_JOIN_ANY) |
				PGS_APPEND | PGS_MERGE_APPEND | PGS_CONSIDER_PARTITIONWISE;
		else
			*pgs_mask_p &= ~(PGS_APPEND | PGS_MERGE_APPEND);
	}
}

/*
 * Enforce restrictions on the join order or join method.
 *
 * Note that, although it is possible to view PARTITIONWISE advice as
 * controlling the join method, we can't enforce it here, because the code
 * path where this executes only deals with join paths that are built directly
 * from a single outer path and a single inner path.
 */
static void
pgpa_planner_apply_join_path_advice(JoinType jointype, uint64 *pgs_mask_p,
									char *plan_name,
									pgpa_join_state *pjs)
{
	int			i = -1;
	Bitmapset  *jo_permit_indexes = NULL;
	Bitmapset  *jo_deny_indexes = NULL;
	Bitmapset  *jm_indexes = NULL;
	bool		jm_conflict = false;
	uint32		join_mask = 0;

	/* Iterate over all possibly-relevant advice. */
	while ((i = bms_next_member(pjs->join_indexes, i)) >= 0)
	{
		pgpa_trove_entry *entry = &pjs->join_entries[i];
		uint32		my_join_mask;

		/* Handle join order advice. */
		if (entry->tag == PGPA_TAG_JOIN_ORDER)
		{
			if (pgpa_join_order_permits_join(pjs->outer_count,
											 pjs->inner_count,
											 pjs->rids,
											 entry))
				jo_permit_indexes = bms_add_member(jo_permit_indexes, i);
			else
				jo_deny_indexes = bms_add_member(jo_deny_indexes, i);
			continue;
		}

		/* Handle join strategy advice. */
		my_join_mask = pgpa_join_strategy_mask_from_advice_tag(entry->tag);
		if (my_join_mask != 0)
		{
			bool		permit;
			bool		restrict_method;

			if (entry->tag == PGPA_TAG_FOREIGN_JOIN)
				permit = pgpa_opaque_join_permits_join(pjs->outer_count,
													   pjs->inner_count,
													   pjs->rids,
													   entry,
													   &restrict_method);
			else
				permit = pgpa_join_method_permits_join(pjs->outer_count,
													   pjs->inner_count,
													   pjs->rids,
													   entry,
													   &restrict_method);
			if (!permit)
				jo_deny_indexes = bms_add_member(jo_deny_indexes, i);
			else if (restrict_method)
			{
				jo_permit_indexes = bms_add_member(jo_permit_indexes, i);
				jm_indexes = bms_add_member(jo_permit_indexes, i);
				if (join_mask != 0 && join_mask != my_join_mask)
					jm_conflict = true;
				join_mask = my_join_mask;
			}
			continue;
		}

		/* Handle semijoin uniqueness advice. */
		if (entry->tag == PGPA_TAG_SEMIJOIN_UNIQUE ||
			entry->tag == PGPA_TAG_SEMIJOIN_NON_UNIQUE)
		{
			bool		advice_unique;
			bool		jt_unique;
			bool		jt_non_unique;
			bool		restrict_method;

			/* Advice wants to unique-ify and use a regular join? */
			advice_unique = (entry->tag == PGPA_TAG_SEMIJOIN_UNIQUE);

			/* Planner is trying to unique-ify and use a regular join? */
			jt_unique = (jointype == JOIN_UNIQUE_INNER ||
						 jointype == JOIN_UNIQUE_OUTER);

			/* Planner is trying a semi-join, without unique-ifying? */
			jt_non_unique = (jointype == JOIN_SEMI ||
							 jointype == JOIN_RIGHT_SEMI);

			/*
			 * These advice tags behave very much like join method advice, in
			 * that they want the inner side of the semijoin to match the
			 * relations listed in the advice. Hence, we test whether join
			 * method advice would enforce a join order restriction here, and
			 * disallow the join if not.
			 *
			 * XXX. Think harder about right semijoins.
			 */
			if (!pgpa_join_method_permits_join(pjs->outer_count,
											   pjs->inner_count,
											   pjs->rids,
											   entry,
											   &restrict_method))
				jo_deny_indexes = bms_add_member(jo_deny_indexes, i);
			else if (restrict_method)
			{
				jo_permit_indexes = bms_add_member(jo_permit_indexes, i);
				if (!jt_unique && !jt_non_unique)
				{
					/*
					 * This doesn't seem to be a semijoin to which SJ_UNIQUE
					 * or SJ_NON_UNIQUE can be applied.
					 */
					entry->flags |= PGPA_TE_INAPPLICABLE;
				}
				else if (advice_unique != jt_unique)
					jo_deny_indexes = bms_add_member(jo_deny_indexes, i);
			}
			continue;
		}
	}

	/*
	 * If the advice indicates both that this join order is permissible and
	 * also that it isn't, then mark advice related to the join order as
	 * conflicting.
	 */
	if (jo_permit_indexes != NULL && jo_deny_indexes != NULL)
	{
		pgpa_trove_set_flags(pjs->join_entries, jo_permit_indexes,
							 PGPA_TE_CONFLICTING);
		pgpa_trove_set_flags(pjs->join_entries, jo_deny_indexes,
							 PGPA_TE_CONFLICTING);
	}

	/*
	 * If more than one join method specification is relevant here and they
	 * differ, mark them all as conflicting.
	 */
	if (jm_conflict)
		pgpa_trove_set_flags(pjs->join_entries, jm_indexes,
							 PGPA_TE_CONFLICTING);

	/*
	 * If we were advised to deny this join order, then do so. However, if we
	 * were also advised to permit it, then do nothing, since the advice
	 * conflicts.
	 */
	if (jo_deny_indexes != NULL && jo_permit_indexes == NULL)
		*pgs_mask_p = 0;

	/*
	 * If we were advised to restrict the join method, then do so. However, if
	 * we got conflicting join method advice or were also advised to reject
	 * this join order completely, then instead do nothing.
	 */
	if (join_mask != 0 && !jm_conflict && jo_deny_indexes == NULL)
		*pgs_mask_p = (*pgs_mask_p & ~PGS_JOIN_ANY) | join_mask;
}

/*
 * Translate an advice tag into a path generation strategy mask.
 *
 * This function can be called with tag types that don't represent join
 * strategies. In such cases, we just return 0, which can't be confused with
 * a valid mask.
 */
static uint64
pgpa_join_strategy_mask_from_advice_tag(pgpa_advice_tag_type tag)
{
	switch (tag)
	{
		case PGPA_TAG_FOREIGN_JOIN:
			return PGS_FOREIGNJOIN;
		case PGPA_TAG_MERGE_JOIN_PLAIN:
			return PGS_MERGEJOIN_PLAIN;
		case PGPA_TAG_MERGE_JOIN_MATERIALIZE:
			return PGS_MERGEJOIN_MATERIALIZE;
		case PGPA_TAG_NESTED_LOOP_PLAIN:
			return PGS_NESTLOOP_PLAIN;
		case PGPA_TAG_NESTED_LOOP_MATERIALIZE:
			return PGS_NESTLOOP_MATERIALIZE;
		case PGPA_TAG_NESTED_LOOP_MEMOIZE:
			return PGS_NESTLOOP_MEMOIZE;
		case PGPA_TAG_HASH_JOIN:
			return PGS_HASHJOIN;
		default:
			return 0;
	}
}

/*
 * Does a certain item of join order advice permit a certain join?
 */
static bool
pgpa_join_order_permits_join(int outer_count, int inner_count,
							 pgpa_identifier *rids,
							 pgpa_trove_entry *entry)
{
	bool		loop = true;
	bool		sublist = false;
	int			length;
	int			outer_length;
	pgpa_advice_target *target = entry->target;
	pgpa_advice_target *prefix_target;

	/* We definitely have at least a partial match for this trove entry. */
	entry->flags |= PGPA_TE_MATCH_PARTIAL;

	/*
	 * Find the innermost sublist that contains all keys; if no sublist does,
	 * then continue processing with the toplevel list.
	 *
	 * For example, if the advice says JOIN_ORDER(t1 t2 (t3 t4 t5)), then we
	 * should evaluate joins that only involve t3, t4, and/or t5 against the
	 * (t3 t4 t5) sublist, and others against the full list.
	 *
	 * Note that (1) outermost sublist is always ordered and (2) whenever we
	 * zoom into an unordered sublist, we instantly accept the proposed join.
	 * If the advice says JOIN_ORDER(t1 t2 {t3 t4 t5}), any approach to
	 * joining t3, t4, and/or t5 is acceptable.
	 */
	while (loop)
	{
		Assert(target->ttype == PGPA_TARGET_ORDERED_LIST);

		loop = false;
		foreach_ptr(pgpa_advice_target, child_target, target->children)
		{
			pgpa_itm_type itm;

			if (child_target->ttype == PGPA_TARGET_IDENTIFIER)
				continue;

			itm = pgpa_identifiers_match_target(outer_count + inner_count,
												rids, child_target);
			if (itm == PGPA_ITM_EQUAL || itm == PGPA_ITM_KEYS_ARE_SUBSET)
			{
				if (child_target->ttype == PGPA_TARGET_ORDERED_LIST)
				{
					target = child_target;
					sublist = true;
					loop = true;
					break;
				}
				else
				{
					Assert(child_target->ttype == PGPA_TARGET_UNORDERED_LIST);
					return true;
				}
			}
		}
	}

	/*
	 * Try to find a prefix of the selected join order list that is exactly
	 * equal to the outer side of the proposed join.
	 */
	length = list_length(target->children);
	prefix_target = palloc0_object(pgpa_advice_target);
	prefix_target->ttype = PGPA_TARGET_ORDERED_LIST;
	for (outer_length = 1; outer_length <= length; ++outer_length)
	{
		pgpa_itm_type itm;

		/* Avoid leaking memory in every loop iteration. */
		if (prefix_target->children != NULL)
			list_free(prefix_target->children);
		prefix_target->children = list_copy_head(target->children,
												 outer_length);

		/* Search, hoping to find an exact match. */
		itm = pgpa_identifiers_match_target(outer_count, rids, prefix_target);
		if (itm == PGPA_ITM_EQUAL)
			break;

		/*
		 * If the prefix of the join order list that we're considering
		 * includes some but not all of the outer rels, we can make the prefix
		 * longer to find an exact match. But the advice hasn't mentioned
		 * everything that's part of our outer rel yet, but has mentioned
		 * things that are not, then this join doesn't match the join order
		 * list.
		 */
		if (itm != PGPA_ITM_TARGETS_ARE_SUBSET)
			return false;
	}

	/*
	 * If the previous looped stopped before the prefix_target included the
	 * entire join order list, then the next member of the join order list
	 * must exactly match the inner side of the join.
	 *
	 * Example: Given JOIN_ORDER(t1 t2 (t3 t4 t5)), if the outer side of the
	 * current join includes only t1, then the inner side must be exactly t2;
	 * if the outer side includes both t1 and t2, then the inner side must
	 * include exactly t3, t4, and t5.
	 */
	if (outer_length < length)
	{
		pgpa_advice_target *inner_target;
		pgpa_itm_type itm;

		inner_target = list_nth(target->children, outer_length);

		itm = pgpa_identifiers_match_target(inner_count, rids + outer_count,
											inner_target);

		/*
		 * Before returning, consider whether we need to mark this entry as
		 * fully matched. If we found every item but one on the lefthand side
		 * of the join and the last item on the righthand side of the join,
		 * then the answer is yes.
		 */
		if (outer_length + 1 == length && itm == PGPA_ITM_EQUAL)
			entry->flags |= PGPA_TE_MATCH_FULL;

		return (itm == PGPA_ITM_EQUAL);
	}

	/*
	 * If we get here, then the outer side of the join includes the entirety
	 * of the join order list. In this case, we behave differently depending
	 * on whether we're looking at the top-level join order list or sublist.
	 * At the top-level, we treat the specified list as mandating that the
	 * actual join order has the given list as a prefix, but a sublist
	 * requires an exact match.
	 *
	 * Exmaple: Given JOIN_ORDER(t1 t2 (t3 t4 t5)), we must start by joining
	 * all five of those relations and in that sequence, but once that is
	 * done, it's OK to join any other rels that are part of the join problem.
	 * This allows a user to specify the driving table and perhaps the first
	 * few things to which it should be joined while leaving the rest of the
	 * join order up the optimizer. But it seems like it would be surprising,
	 * given that specification, if the user could add t6 to the (t3 t4 t5)
	 * sub-join, so we don't allow that. If we did want to allow it, the logic
	 * earlier in this function would require substantial adjustment: we could
	 * allow the t3-t4-t5-t6 join to be built here, but the next step of
	 * joining t1-t2 to the result would still be rejected.
	 */
	return !sublist;
}

/*
 * Does a certain item of join method advice permit a certain join?
 *
 * Advice such as HASH_JOIN((x y)) means that there should be a hash join with
 * exactly x and y on the inner side. Obviously, this means that if we are
 * considering a join with exactly x and y on the inner side, we should enforce
 * the use of a hash join. However, it also means that we must reject some
 * incompatible join orders entirely.  For example, a join with exactly x
 * and y on the outer side shouldn't be allowed, because such paths might win
 * over the advice-driven path on cost.
 *
 * To accommodate these requirements, this function returns true if the join
 * should be allowed and false if it should not. Furthermore, *restrict_method
 * is set to true if the join method should be enforced and false if not.
 */
static bool
pgpa_join_method_permits_join(int outer_count, int inner_count,
							  pgpa_identifier *rids,
							  pgpa_trove_entry *entry,
							  bool *restrict_method)
{
	pgpa_advice_target *target = entry->target;
	pgpa_itm_type inner_itm;
	pgpa_itm_type outer_itm;
	pgpa_itm_type join_itm;

	/* We definitely have at least a partial match for this trove entry. */
	entry->flags |= PGPA_TE_MATCH_PARTIAL;

	*restrict_method = false;

	/*
	 * If our inner rel mentions exactly the same relations as the advice
	 * target, allow the join and enforce the join method restriction.
	 *
	 * If our inner rel mentions a superset of the target relations, allow the
	 * join. The join we care about has already taken place, and this advice
	 * imposes no further restrictions.
	 */
	inner_itm = pgpa_identifiers_match_target(inner_count,
											  rids + outer_count,
											  target);
	if (inner_itm == PGPA_ITM_EQUAL)
	{
		entry->flags |= PGPA_TE_MATCH_FULL;
		*restrict_method = true;
		return true;
	}
	else if (inner_itm == PGPA_ITM_TARGETS_ARE_SUBSET)
		return true;

	/*
	 * If our outer rel mentions a supserset of the relations in the advice
	 * target, no restrictions apply. The join we care has already taken
	 * place, and this advice imposes no further restrictions.
	 *
	 * On the other hand, if our outer rel mentions exactly the relations
	 * mentioned in the advice target, the planner is trying to reverse the
	 * sides of the join as compared with our desired outcome. Reject that.
	 */
	outer_itm = pgpa_identifiers_match_target(outer_count,
											  rids, target);
	if (outer_itm == PGPA_ITM_TARGETS_ARE_SUBSET)
		return true;
	else if (outer_itm == PGPA_ITM_EQUAL)
		return false;

	/*
	 * If the advice target mentions only a single relation, the test below
	 * cannot ever pass, so save some work by exiting now.
	 */
	if (target->ttype == PGPA_TARGET_IDENTIFIER)
		return false;

	/*
	 * If everything in the joinrel is appears in the advice target, we're
	 * below the level of the join we want to control.
	 *
	 * For example, HASH_JOIN((x y)) doesn't restrict how x and y can be
	 * joined.
	 *
	 * This lookup shouldn't return PGPA_ITM_DISJOINT, because any such advice
	 * should not have been returned from the trove in the first place.
	 */
	join_itm = pgpa_identifiers_match_target(outer_count + inner_count,
											 rids, target);
	Assert(join_itm != PGPA_ITM_DISJOINT);
	if (join_itm == PGPA_ITM_KEYS_ARE_SUBSET ||
		join_itm == PGPA_ITM_EQUAL)
		return true;

	/*
	 * We've already permitted all allowable cases, so reject this.
	 *
	 * If we reach this point, then the advice overlaps with this join but
	 * isn't entirely contained within either side, and there's also at least
	 * one relation present in the join that isn't mentioned by the advice.
	 *
	 * For instance, in the HASH_JOIN((x y)) example, we would reach here if x
	 * were on one side of the join, y on the other, and at least one of the
	 * two sides also included some other relation, say t. In that case,
	 * accepting this join would allow the (x y t) joinrel to contain
	 * non-disabled paths that do not put (x y) on the inner side of a hash
	 * join; we could instead end up with something like (x JOIN t) JOIN y.
	 */
	return false;
}

/*
 * Does advice concerning an opaque join permit a certain join?
 *
 * By an opaque join, we mean one where the exact mechanism by which the
 * join is performed is not visible to PostgreSQL. Currently this is the
 * case only for foreign joins: FOREIGN_JOIN((x y z)) means that x, y, and
 * z are joined on the remote side, but we know nothing about the join order
 * or join methods used over there.
 */
static bool
pgpa_opaque_join_permits_join(int outer_count, int inner_count,
							  pgpa_identifier *rids,
							  pgpa_trove_entry *entry,
							  bool *restrict_method)
{
	pgpa_advice_target *target = entry->target;
	pgpa_itm_type join_itm;

	/* We definitely have at least a partial match for this trove entry. */
	entry->flags |= PGPA_TE_MATCH_PARTIAL;

	*restrict_method = false;

	join_itm = pgpa_identifiers_match_target(outer_count + inner_count,
											 rids, target);
	if (join_itm == PGPA_ITM_EQUAL)
	{
		/*
		 * We have an exact match, and should therefore allow the join and
		 * enforce the use of the relevant opaque join method.
		 */
		entry->flags |= PGPA_TE_MATCH_FULL;
		*restrict_method = true;
		return true;
	}

	if (join_itm == PGPA_ITM_KEYS_ARE_SUBSET ||
		join_itm == PGPA_ITM_TARGETS_ARE_SUBSET)
	{
		/*
		 * If join_itm == PGPA_ITM_TARGETS_ARE_SUBSET, then the join we care
		 * about has already taken place and no further restrictions apply.
		 *
		 * If join_itm == PGPA_ITM_KEYS_ARE_SUBSET, we're still building up to
		 * the join we care about and have not introduced any extraneous
		 * relations not named in the advice. Note that ForeignScan paths for
		 * joins are built up from ForeignScan paths from underlying joins and
		 * scans, so we must not disable this join when considering a subset
		 * of the relations we ultimately want.
		 */
		return true;
	}

	/*
	 * The advice overlaps the join, but at least one relation is present in
	 * the join that isn't mentioned by the advice. We want to disable such
	 * paths so that we actually push down the join as intended.
	 */
	return false;
}

/*
 * Apply scan advice to a RelOptInfo.
 *
 * XXX. For bitmap heap scans, we're just ignoring the index information from
 * the advice. That's not cool.
 */
static void
pgpa_planner_apply_scan_advice(RelOptInfo *rel,
							   pgpa_trove_entry *scan_entries,
							   Bitmapset *scan_indexes,
							   pgpa_trove_entry *rel_entries,
							   Bitmapset *rel_indexes)
{
	bool		gather_conflict = false;
	Bitmapset  *gather_partial_match = NULL;
	Bitmapset  *gather_full_match = NULL;
	int			i = -1;
	pgpa_trove_entry *scan_entry = NULL;
	int			flags;
	bool		scan_type_conflict = false;
	Bitmapset  *scan_type_indexes = NULL;
	Bitmapset  *scan_type_rel_indexes = NULL;
	uint64		gather_mask = 0;
	uint64		scan_type = 0;

	/* Scrutinize available scan advice. */
	while ((i = bms_next_member(scan_indexes, i)) >= 0)
	{
		pgpa_trove_entry *my_entry = &scan_entries[i];
		uint64		my_scan_type = 0;

		/* Translate our advice tags to a scan strategy advice value. */
		if (my_entry->tag == PGPA_TAG_BITMAP_HEAP_SCAN)
			my_scan_type = PGS_BITMAPSCAN;
		else if (my_entry->tag == PGPA_TAG_INDEX_ONLY_SCAN)
			my_scan_type = PGS_INDEXONLYSCAN | PGS_CONSIDER_INDEXONLY;
		else if (my_entry->tag == PGPA_TAG_INDEX_SCAN)
			my_scan_type = PGS_INDEXSCAN;
		else if (my_entry->tag == PGPA_TAG_SEQ_SCAN)
			my_scan_type = PGS_SEQSCAN;
		else if (my_entry->tag == PGPA_TAG_TID_SCAN)
			my_scan_type = PGS_TIDSCAN;

		/*
		 * If this is understandable scan advice, hang on to the entry, the
		 * inferred scan type type, and the index at which we found it.
		 *
		 * Also make a note if we see conflicting scan type advice. Note that
		 * we regard two index specifications as conflicting unless they match
		 * exactly. In theory, perhaps we could regard INDEX_SCAN(a c) and
		 * INDEX_SCAN(a b.c) as non-conflicting if it happens that the only
		 * index named c is in schema b, but it doesn't seem worth the code.
		 */
		if (my_scan_type != 0)
		{
			if (scan_type != 0 && scan_type != my_scan_type)
				scan_type_conflict = true;
			if (!scan_type_conflict && scan_entry != NULL &&
				my_entry->target->itarget != NULL &&
				scan_entry->target->itarget != NULL &&
				!pgpa_index_targets_equal(scan_entry->target->itarget,
										  my_entry->target->itarget))
				scan_type_conflict = true;
			scan_entry = my_entry;
			scan_type = my_scan_type;
			scan_type_indexes = bms_add_member(scan_type_indexes, i);
		}
	}

	/* Scrutinize available gather-related and partitionwise advice. */
	i = -1;
	while ((i = bms_next_member(rel_indexes, i)) >= 0)
	{
		pgpa_trove_entry *my_entry = &rel_entries[i];
		uint64		my_gather_mask = 0;
		bool		just_one_rel;

		just_one_rel = my_entry->target->ttype == PGPA_TARGET_IDENTIFIER
			|| list_length(my_entry->target->children) == 1;

		/*
		 * PARTITIONWISE behaves like a scan type, except that if there's more
		 * than one relation targeted, it has no effect at this level.
		 */
		if (my_entry->tag == PGPA_TAG_PARTITIONWISE)
		{
			if (just_one_rel)
			{
				const uint64 my_scan_type = PGS_APPEND | PGS_MERGE_APPEND;

				if (scan_type != 0 && scan_type != my_scan_type)
					scan_type_conflict = true;
				scan_entry = my_entry;
				scan_type = my_scan_type;
				scan_type_rel_indexes =
					bms_add_member(scan_type_rel_indexes, i);
			}
			continue;
		}

		/*
		 * GATHER and GATHER_MERGE applied to a single rel mean that we should
		 * use the correspondings strategy here, while applying either to more
		 * than one rel means we should not use those strategies here, but
		 * rather at the level of the joinrel that corresponds to what was
		 * specified. NO_GATHER can only be applied to single rels.
		 *
		 * Note that setting PGS_CONSIDER_NONPARTIAL in my_gather_mask is
		 * equivalent to allowing the non-use of either form of Gather here.
		 */
		if (my_entry->tag == PGPA_TAG_GATHER ||
			my_entry->tag == PGPA_TAG_GATHER_MERGE)
		{
			if (!just_one_rel)
				my_gather_mask = PGS_CONSIDER_NONPARTIAL;
			else if (my_entry->tag == PGPA_TAG_GATHER)
				my_gather_mask = PGS_GATHER;
			else
				my_gather_mask = PGS_GATHER_MERGE;
		}
		else if (my_entry->tag == PGPA_TAG_NO_GATHER)
		{
			Assert(just_one_rel);
			my_gather_mask = PGS_CONSIDER_NONPARTIAL;
		}

		/*
		 * If we set my_gather_mask up above, then we (1) make a note if the
		 * advice conflicted, (2) remember the mask value, and (3) remember
		 * whether this was a full or partial match.
		 */
		if (my_gather_mask != 0)
		{
			if (gather_mask != 0 && gather_mask != my_gather_mask)
				gather_conflict = true;
			gather_mask = my_gather_mask;
			if (just_one_rel)
				gather_full_match = bms_add_member(gather_full_match, i);
			else
				gather_partial_match = bms_add_member(gather_partial_match, i);
		}
	}

	/* Enforce choice of index. */
	if (scan_entry != NULL && !scan_type_conflict &&
		(scan_entry->tag == PGPA_TAG_INDEX_SCAN ||
		 scan_entry->tag == PGPA_TAG_INDEX_ONLY_SCAN))
	{
		pgpa_index_target *itarget = scan_entry->target->itarget;
		IndexOptInfo *matched_index = NULL;

		Assert(itarget->itype == PGPA_INDEX_NAME);

		foreach_node(IndexOptInfo, index, rel->indexlist)
		{
			char	   *relname = get_rel_name(index->indexoid);
			Oid			nspoid = get_rel_namespace(index->indexoid);
			char	   *relnamespace = get_namespace_name(nspoid);

			if (strcmp(itarget->indname, relname) == 0 &&
				(itarget->indnamespace == NULL ||
				 strcmp(itarget->indnamespace, relnamespace) == 0))
			{
				matched_index = index;
				break;
			}
		}

		if (matched_index == NULL)
		{
			/* Don't force the scan type if the index doesn't exist. */
			scan_type = 0;

			/* Mark advice as inapplicable. */
			pgpa_trove_set_flags(scan_entries, scan_type_indexes,
								 PGPA_TE_INAPPLICABLE);
		}
		else
		{
			/* Retain this index and discard the rest. */
			rel->indexlist = list_make1(matched_index);
		}
	}

	/*
	 * Mark all the scan method entries as fully matched; and if they specify
	 * different things, mark them all as conflicting.
	 */
	flags = PGPA_TE_MATCH_PARTIAL | PGPA_TE_MATCH_FULL;
	if (scan_type_conflict)
		flags |= PGPA_TE_CONFLICTING;
	pgpa_trove_set_flags(scan_entries, scan_type_indexes, flags);
	pgpa_trove_set_flags(rel_entries, scan_type_rel_indexes, flags);

	/*
	 * Mark every Gather-related piece of advice as partially matched. Mark
	 * the ones that included this relation as a target by itself as fully
	 * matched. If there was a conflict, mark them all as conflicting.
	 */
	flags = PGPA_TE_MATCH_PARTIAL;
	if (gather_conflict)
		flags |= PGPA_TE_CONFLICTING;
	pgpa_trove_set_flags(rel_entries, gather_partial_match, flags);
	flags |= PGPA_TE_MATCH_FULL;
	pgpa_trove_set_flags(rel_entries, gather_full_match, flags);

	/* If there is a non-conflicting scan specification, enforce it. */
	if (scan_type != 0 && !scan_type_conflict)
	{
		rel->pgs_mask &=
			~(PGS_SCAN_ANY | PGS_APPEND | PGS_MERGE_APPEND |
			  PGS_CONSIDER_INDEXONLY);
		rel->pgs_mask |= scan_type;
	}

	/* If there is a non-conflicting gather specification, enforce it. */
	if (gather_mask != 0 && !gather_conflict)
	{
		rel->pgs_mask &=
			~(PGS_GATHER | PGS_GATHER_MERGE | PGS_CONSIDER_NONPARTIAL);
		rel->pgs_mask |= gather_mask;
	}
}

/*
 * Add feedback entries to for one trove slice to the provided list and
 * return the resulting list.
 *
 * Feedback entries are generated from the trove entry's flags. It's assumed
 * that the caller has already set all relevant flags with the exception of
 * PGPA_TE_FAILED. We set that flag here if appropriate.
 */
static List *
pgpa_planner_append_feedback(List *list, pgpa_trove *trove,
							 pgpa_trove_lookup_type type,
							 pgpa_identifier *rt_identifiers,
							 pgpa_plan_walker_context *walker)
{
	pgpa_trove_entry *entries;
	int			nentries;
	StringInfoData buf;

	initStringInfo(&buf);
	pgpa_trove_lookup_all(trove, type, &entries, &nentries);
	for (int i = 0; i < nentries; ++i)
	{
		pgpa_trove_entry *entry = &entries[i];
		DefElem    *item;

		/*
		 * If this entry was fully matched, check whether generating advice
		 * from this plan would produce such an entry. If not, label the entry
		 * as failed.
		 */
		if ((entry->flags & PGPA_TE_MATCH_FULL) != 0 &&
			!pgpa_walker_would_advise(walker, rt_identifiers,
									  entry->tag, entry->target))
			entry->flags |= PGPA_TE_FAILED;

		item = makeDefElem(pgpa_cstring_trove_entry(entry),
						   (Node *) makeInteger(entry->flags), -1);
		list = lappend(list, item);
	}

	return list;
}

#ifdef USE_ASSERT_CHECKING

/*
 * Fast hash function for a key consisting of an RTI and plan name.
 */
static uint32
pgpa_ri_checker_hash_key(pgpa_ri_checker_key key)
{
	fasthash_state hs;
	int			sp_len;

	fasthash_init(&hs, 0);

	hs.accum = key.rti;
	fasthash_combine(&hs);

	/* plan_name can be NULL */
	if (key.plan_name == NULL)
		sp_len = 0;
	else
		sp_len = fasthash_accum_cstring(&hs, key.plan_name);

	/* hashfn_unstable.h recommends using string length as tweak */
	return fasthash_final32(&hs, sp_len);
}

#endif

/*
 * Save the range table identifier for one relation for future cross-checking.
 */
static void
pgpa_ri_checker_save(pgpa_planner_state *pps, PlannerInfo *root,
					 RelOptInfo *rel)
{
#ifdef USE_ASSERT_CHECKING
	pgpa_ri_checker_key key;
	pgpa_ri_checker *check;
	pgpa_identifier rid;
	const char *rid_string;
	bool		found;

	key.rti = bms_singleton_member(rel->relids);
	key.plan_name = root->plan_name;
	pgpa_compute_identifier_by_rti(root, key.rti, &rid);
	rid_string = pgpa_identifier_string(&rid);
	check = pgpa_ri_check_insert(pps->ri_check_hash, key, &found);
	Assert(!found || strcmp(check->rid_string, rid_string) == 0);
	check->rid_string = rid_string;
#endif
}

/*
 * Validate that the range table identifiers we were able to generate during
 * planning match the ones we generated from the final plan.
 */
static void
pgpa_ri_checker_validate(pgpa_planner_state *pps, PlannedStmt *pstmt)
{
#ifdef USE_ASSERT_CHECKING
	pgpa_identifier *rt_identifiers;
	pgpa_ri_check_iterator it;
	pgpa_ri_checker *check;

	/* Create identifiers from the planned statement. */
	rt_identifiers = pgpa_create_identifiers_for_planned_stmt(pstmt);

	/* Iterate over identifiers created during planning, so we can compare. */
	pgpa_ri_check_start_iterate(pps->ri_check_hash, &it);
	while ((check = pgpa_ri_check_iterate(pps->ri_check_hash, &it)) != NULL)
	{
		int			rtoffset = 0;
		const char *rid_string;
		Index		flat_rti;

		/*
		 * If there's no plan name associated with this entry, then the
		 * rtoffset is 0. Otherwise, we can search the SubPlanRTInfo list to
		 * find the rtoffset.
		 */
		if (check->key.plan_name != NULL)
		{
			foreach_node(SubPlanRTInfo, rtinfo, pstmt->subrtinfos)
			{
				/*
				 * If rtinfo->dummy is set, then the subquery's range table
				 * will only have been partially copied to the final range
				 * table. Specifically, only RTE_RELATION entries and
				 * RTE_SUBQUERY entries that were once RTE_RELATION entries
				 * will be copied, as per add_rtes_to_flat_rtable. Therefore,
				 * there's no fixed rtoffset that we can apply to the RTIs
				 * used during planning to locate the corresponding relations
				 * in the final rtable.
				 *
				 * With more complex logic, we could work around that problem
				 * by remembering the whole contents of the subquery's rtable
				 * during planning, determining which of those would have been
				 * copied to the final rtable, and matching them up. But it
				 * doesn't seem like a worthwhile endeavor for right now,
				 * because RTIs from such subqueries won't appear in the plan
				 * tree itself, just in the range table. Hence, we can neither
				 * generate nor accept advice for them.
				 */
				if (strcmp(check->key.plan_name, rtinfo->plan_name) == 0
					&& !rtinfo->dummy)
				{
					rtoffset = rtinfo->rtoffset;
					Assert(rtoffset > 0);
					break;
				}
			}

			/*
			 * It's not an error if we don't find the plan name: that just
			 * means that we planned a subplan by this name but it ended up
			 * being a dummy subplan and so wasn't included in the final plan
			 * tree.
			 */
			if (rtoffset == 0)
				continue;
		}

		/*
		 * check->key.rti is the RTI that we saw prior to range-table
		 * flattening, so we must add the appropriate RT offset to get the
		 * final RTI.
		 */
		flat_rti = check->key.rti + rtoffset;
		Assert(flat_rti <= list_length(pstmt->rtable));

		/* Assert that the string we compute now matches the previous one. */
		rid_string = pgpa_identifier_string(&rt_identifiers[flat_rti - 1]);
		Assert(strcmp(rid_string, check->rid_string) == 0);
	}
#endif
}
