/*-------------------------------------------------------------------------
 *
 * planmain.c
 *	  Routines to plan a single query
 *
 * What's in a name, anyway?  The top-level entry point of the planner/
 * optimizer is over in planner.c, not here as you might think from the
 * file name.  But this is the main code for planning a basic join operation,
 * shorn of features like subselects, inheritance, aggregates, grouping,
 * and so on.  (Those are the things planner.c deals with.)
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/planmain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/inherit.h"
#include "optimizer/orclauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"

static void add_inherited_target_child_roots(PlannerInfo *root);
static PlannerInfo *adjust_inherited_target_child_root(PlannerInfo *root,
									AppendRelInfo *appinfo);

/*
 * query_planner
 *	  Generate a path (that is, a simplified plan) for a basic query,
 *	  which may involve joins but not any fancier features.
 *
 * Since query_planner does not handle the toplevel processing (grouping,
 * sorting, etc) it cannot select the best path by itself.  Instead, it
 * returns the RelOptInfo for the top level of joining, and the caller
 * (grouping_planner) can choose among the surviving paths for the rel.
 *
 * root describes the query to plan
 * tlist is the target list the query should produce
 *		(this is NOT necessarily root->parse->targetList!)
 * qp_callback is a function to compute query_pathkeys once it's safe to do so
 * qp_extra is optional extra data to pass to qp_callback
 *
 * Note: the PlannerInfo node also includes a query_pathkeys field, which
 * tells query_planner the sort order that is desired in the final output
 * plan.  This value is *not* available at call time, but is computed by
 * qp_callback once we have completed merging the query's equivalence classes.
 * (We cannot construct canonical pathkeys until that's done.)
 */
RelOptInfo *
query_planner(PlannerInfo *root, List *tlist,
			  query_pathkeys_callback qp_callback, void *qp_extra)
{
	Query	   *parse = root->parse;
	List	   *joinlist;
	RelOptInfo *final_rel;
	Index		rti;

	/*
	 * If the query has an empty join tree, then it's something easy like
	 * "SELECT 2+2;" or "INSERT ... VALUES()".  Fall through quickly.
	 */
	if (parse->jointree->fromlist == NIL)
	{
		/* We need a dummy joinrel to describe the empty set of baserels */
		final_rel = build_empty_join_rel(root);

		/*
		 * If query allows parallelism in general, check whether the quals are
		 * parallel-restricted.  (We need not check final_rel->reltarget
		 * because it's empty at this point.  Anything parallel-restricted in
		 * the query tlist will be dealt with later.)
		 */
		if (root->glob->parallelModeOK)
			final_rel->consider_parallel =
				is_parallel_safe(root, parse->jointree->quals);

		/* The only path for it is a trivial Result path */
		add_path(final_rel, (Path *)
				 create_result_path(root, final_rel,
									final_rel->reltarget,
									(List *) parse->jointree->quals));

		/* Select cheapest path (pretty easy in this case...) */
		set_cheapest(final_rel);

		/*
		 * We still are required to call qp_callback, in case it's something
		 * like "SELECT 2+2 ORDER BY 1".
		 */
		root->canon_pathkeys = NIL;
		(*qp_callback) (root, qp_extra);

		return final_rel;
	}

	/*
	 * Init planner lists to empty.
	 *
	 * NOTE: append_rel_list was set up by subquery_planner, so do not touch
	 * here.
	 */
	root->join_rel_list = NIL;
	root->join_rel_hash = NULL;
	root->join_rel_level = NULL;
	root->join_cur_level = 0;
	root->canon_pathkeys = NIL;
	root->left_join_clauses = NIL;
	root->right_join_clauses = NIL;
	root->full_join_clauses = NIL;
	root->join_info_list = NIL;
	root->placeholder_list = NIL;
	root->fkey_list = NIL;
	root->initial_rels = NIL;

	/*
	 * Make a flattened version of the rangetable for faster access (this is
	 * OK because the rangetable won't change any more), and set up an empty
	 * array for indexing base relations.
	 */
	setup_simple_rel_arrays(root);

	/*
	 * Populate append_rel_array with each AppendRelInfo to allow direct
	 * lookups by child relid.
	 */
	setup_append_rel_array(root);

	/*
	 * Construct RelOptInfo nodes for all base relations in query, and
	 * indirectly for all appendrel member relations ("other rels").  This
	 * will give us a RelOptInfo for every "simple" (non-join) rel involved in
	 * the query.
	 *
	 * Note: the reason we find the rels by searching the jointree and
	 * appendrel list, rather than just scanning the rangetable, is that the
	 * rangetable may contain RTEs for rels not actively part of the query,
	 * for example views.  We don't want to make RelOptInfos for them.
	 */
	add_base_rels_to_query(root, (Node *) parse->jointree);

	/*
	 * Examine the targetlist and join tree, adding entries to baserel
	 * targetlists for all referenced Vars, and generating PlaceHolderInfo
	 * entries for all referenced PlaceHolderVars.  Restrict and join clauses
	 * are added to appropriate lists belonging to the mentioned relations. We
	 * also build EquivalenceClasses for provably equivalent expressions. The
	 * SpecialJoinInfo list is also built to hold information about join order
	 * restrictions.  Finally, we form a target joinlist for make_one_rel() to
	 * work from.
	 */
	build_base_rel_tlists(root, tlist);

	find_placeholders_in_jointree(root);

	find_lateral_references(root);

	joinlist = deconstruct_jointree(root);

	/*
	 * Reconsider any postponed outer-join quals now that we have built up
	 * equivalence classes.  (This could result in further additions or
	 * mergings of classes.)
	 */
	reconsider_outer_join_clauses(root);

	/*
	 * If we formed any equivalence classes, generate additional restriction
	 * clauses as appropriate.  (Implied join clauses are formed on-the-fly
	 * later.)
	 */
	generate_base_implied_equalities(root);

	/*
	 * We have completed merging equivalence sets, so it's now possible to
	 * generate pathkeys in canonical form; so compute query_pathkeys and
	 * other pathkeys fields in PlannerInfo.
	 */
	(*qp_callback) (root, qp_extra);

	/*
	 * Examine any "placeholder" expressions generated during subquery pullup.
	 * Make sure that the Vars they need are marked as needed at the relevant
	 * join level.  This must be done before join removal because it might
	 * cause Vars or placeholders to be needed above a join when they weren't
	 * so marked before.
	 */
	fix_placeholder_input_needed_levels(root);

	/*
	 * Remove any useless outer joins.  Ideally this would be done during
	 * jointree preprocessing, but the necessary information isn't available
	 * until we've built baserel data structures and classified qual clauses.
	 */
	joinlist = remove_useless_joins(root, joinlist);

	/*
	 * Also, reduce any semijoins with unique inner rels to plain inner joins.
	 * Likewise, this can't be done until now for lack of needed info.
	 */
	reduce_unique_semijoins(root);

	/*
	 * Now distribute "placeholders" to base rels as needed.  This has to be
	 * done after join removal because removal could change whether a
	 * placeholder is evaluable at a base rel.
	 */
	add_placeholders_to_base_rels(root);

	/*
	 * Construct the lateral reference sets now that we have finalized
	 * PlaceHolderVar eval levels.
	 */
	create_lateral_join_info(root);

	/*
	 * Match foreign keys to equivalence classes and join quals.  This must be
	 * done after finalizing equivalence classes, and it's useful to wait till
	 * after join removal so that we can skip processing foreign keys
	 * involving removed relations.
	 */
	match_foreign_keys_to_quals(root);

	/*
	 * Look for join OR clauses that we can extract single-relation
	 * restriction OR clauses from.
	 */
	extract_restriction_or_clauses(root);

	/*
	 * Construct the all_baserels Relids set.
	 */
	root->all_baserels = NULL;
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *brel = root->simple_rel_array[rti];

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (brel == NULL)
			continue;

		Assert(brel->relid == rti); /* sanity check on array */

		/* ignore RTEs that are "other rels" */
		if (brel->reloptkind != RELOPT_BASEREL)
			continue;

		root->all_baserels = bms_add_member(root->all_baserels, brel->relid);
	}

	/*
	 * Add child subroots needed to use during planning for individual child
	 * targets
	 */
	if (root->inherited_update)
	{
		root->inh_target_child_roots = (PlannerInfo **)
										palloc0(root->simple_rel_array_size *
												sizeof(PlannerInfo *));
		add_inherited_target_child_roots(root);
	}

	/*
	 * Ready to do the primary planning.
	 */
	final_rel = make_one_rel(root, joinlist);

	/*
	 * Check that we got at least one usable path.  In the case of an
	 * inherited update/delete operation, no path has been created for
	 * the query's actual target relation yet.
	 */
	if (!root->inherited_update &&
		(!final_rel ||
		 !final_rel->cheapest_total_path ||
		 final_rel->cheapest_total_path->param_info != NULL))
		elog(ERROR, "failed to construct the join relation");

	return final_rel;
}

/*
 * add_inherited_target_child_roots
 *		Add PlannerInfos for inheritance target children
 */
static void
add_inherited_target_child_roots(PlannerInfo *root)
{
	Index		resultRelation = root->parse->resultRelation;
	ListCell   *lc;

	Assert(root->inh_target_child_roots != NULL);

	foreach(lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst(lc);
		RangeTblEntry *childRTE;
		PlannerInfo   *subroot;

		if (appinfo->parent_relid != resultRelation)
			continue;

		childRTE = root->simple_rte_array[appinfo->child_relid];

		/*
		 * Create a PlannerInfo for processing this child target relation
		 * with.
		 */
		subroot = adjust_inherited_target_child_root(root, appinfo);

		if (childRTE->inh)
			add_inherited_target_child_roots(subroot);

		root->inh_target_child_roots[appinfo->child_relid] = subroot;
	}
}

/*
 * add_inherit_target_child_root
 *		This translates query to match the child given by appinfo and
 *		puts it in a PlannerInfo that will be used for planning the child
 *
 * The child PlannerInfo reuses most of the parent PlannerInfo's fields
 * unchanged, except unexpanded_tlist and processed_tlist are based on the
 * child relation.
 */
static PlannerInfo *
adjust_inherited_target_child_root(PlannerInfo *root, AppendRelInfo *appinfo)
{
	PlannerInfo *subroot;
	List	   *tlist;
	ListCell   *lc;

	Assert(root->parse->commandType == CMD_UPDATE ||
		   root->parse->commandType == CMD_DELETE);

	/* Translate the original query's expressions to this child. */
	subroot = makeNode(PlannerInfo);
	memcpy(subroot, root, sizeof(PlannerInfo));

	/*
	 * Restore the unexpanded tlist for translation, so that child's
	 * query contains targetList numbered (resnos) per its own
	 * TupleDesc, which adjust_inherited_tlist ensures.
	 */
	root->parse->targetList = root->unexpanded_tlist;
	subroot->parse = (Query *) adjust_appendrel_attrs(root,
													  (Node *) root->parse,
													  1, &appinfo);

	/*
	 * Save the original unexpanded targetlist in child subroot, just as
	 * we did for the parent root, so that this child's own children can use
	 * it.  Must use copy because subroot->parse->targetList will be modified
	 * soon.
	 */
	subroot->unexpanded_tlist = list_copy(subroot->parse->targetList);

	/*
	 * Apply planner's expansion of targetlist, such as adding various junk
	 * column, filling placeholder entries for dropped columns, etc., all of
	 * which occurs with the child's TupleDesc.
	 */
	tlist = preprocess_targetlist(subroot);
	subroot->processed_tlist = tlist;

	/* Add any newly added Vars to the child RelOptInfo. */
	build_base_rel_tlists(subroot, tlist);

	/*
	 * Adjust all_baserels to replace the original target relation with the
	 * child target relation.  Copy it before modifying though.
	 */
	subroot->all_baserels = bms_copy(root->all_baserels);
	subroot->all_baserels = bms_del_member(subroot->all_baserels,
										   root->parse->resultRelation);
	subroot->all_baserels = bms_add_member(subroot->all_baserels,
										   subroot->parse->resultRelation);

	/*
	 * Child root should get its own copy of ECs, because they'll be modified
	 * to replace parent EC expressions by child expressions in
	 * add_child_rel_equivalences.
	 */
	subroot->eq_classes = NIL;
	foreach(lc, root->eq_classes)
	{
		EquivalenceClass *ec = lfirst(lc);
		EquivalenceClass *new_ec = makeNode(EquivalenceClass);

		memcpy(new_ec, ec, sizeof(EquivalenceClass));
		new_ec->ec_members = list_copy(ec->ec_members);
		subroot->eq_classes = lappend(subroot->eq_classes, new_ec);
	}

	return subroot;
}
