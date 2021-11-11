/*-------------------------------------------------------------------------
 *
 * execMerge.c
 *	  routines to handle Merge nodes relating to the MERGE command
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execMerge.c
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/execMerge.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"

static bool ExecMergeMatched(ModifyTableState *mtstate,
							 ResultRelInfo *resultRelInfo,
							 EState *estate,
							 TupleTableSlot *slot,
							 ItemPointer tupleid);
static void ExecMergeNotMatched(ModifyTableState *mtstate,
								ResultRelInfo *resultRelInfo,
								EState *estate,
								TupleTableSlot *slot);

/*
 * Perform MERGE.
 */
void
ExecMerge(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo,
		  EState *estate, TupleTableSlot *slot)
{
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	ItemPointer tupleid;
	ItemPointerData tuple_ctid;
	char		relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
	bool		matched = false;
	Datum		datum;
	bool		isNull;

	Assert(relkind == RELKIND_RELATION || relkind == RELKIND_PARTITIONED_TABLE);

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * We run a JOIN between the target relation and the source relation to
	 * find a set of candidate source rows that has matching row in the target
	 * table and a set of candidate source rows that does not have matching
	 * row in the target table. If the join returns us a tuple with target
	 * relation's tid set, that implies that the join found a matching row for
	 * the given source tuple. This case triggers the WHEN MATCHED clause of
	 * the MERGE. Whereas a NULL in the target relation's ctid column
	 * indicates a NOT MATCHED case.
	 */
	datum = ExecGetJunkAttribute(slot,
								 resultRelInfo->ri_RowIdAttNo,
								 &isNull);

	if (!isNull)
	{
		matched = true;
		tuple_ctid = *((ItemPointer) DatumGetPointer(datum));
		tupleid = &tuple_ctid;
	}
	else
	{
		matched = false;
		tupleid = NULL;			/* we don't need it for NOT MATCHED actions */
	}

	/*
	 * If we are dealing with a WHEN MATCHED case, we execute the first action
	 * for which the additional WHEN MATCHED AND quals pass. If an action
	 * without quals is found, that action is executed.
	 *
	 * Similarly, if we are dealing with WHEN NOT MATCHED case, we look at the
	 * given WHEN NOT MATCHED actions in sequence until one passes.
	 *
	 * Things get interesting in case of concurrent update/delete of the
	 * target tuple. Such concurrent update/delete is detected while we are
	 * executing a WHEN MATCHED action.
	 *
	 * A concurrent update can:
	 *
	 * 1. modify the target tuple so that it no longer satisfies the
	 * additional quals attached to the current WHEN MATCHED action OR
	 *
	 * In this case, we are still dealing with a WHEN MATCHED case, but we
	 * should recheck the list of WHEN MATCHED actions and choose the first
	 * one that satisfies the new target tuple.
	 *
	 * 2. modify the target tuple so that the join quals no longer pass and
	 * hence the source tuple no longer has a match.
	 *
	 * In the second case, the source tuple no longer matches the target
	 * tuple, so we now instead find a qualifying WHEN NOT MATCHED action to
	 * execute.
	 *
	 * A concurrent delete, changes a WHEN MATCHED case to WHEN NOT MATCHED.
	 *
	 * ExecMergeMatched takes care of following the update chain and
	 * re-finding the qualifying WHEN MATCHED action, as long as the updated
	 * target tuple still satisfies the join quals i.e. it still remains a
	 * WHEN MATCHED case. If the tuple gets deleted or the join quals fail, it
	 * returns and we try ExecMergeNotMatched. Given that ExecMergeMatched
	 * always make progress by following the update chain and we never switch
	 * from ExecMergeNotMatched to ExecMergeMatched, there is no risk of a
	 * livelock.
	 */
	if (matched)
		matched = ExecMergeMatched(mtstate, resultRelInfo, estate,
								   slot, tupleid);

	/*
	 * Either we were dealing with a NOT MATCHED tuple or
	 * ExecMergeNotMatched() returned "false", indicating the previously
	 * MATCHED tuple is no longer a matching tuple.
	 */
	if (!matched)
		ExecMergeNotMatched(mtstate, resultRelInfo, estate, slot);
}

/*
 * Check and execute the first qualifying MATCHED action. The current target
 * tuple is identified by tupleid.
 *
 * We start from the first WHEN MATCHED action and check if the WHEN AND quals
 * pass, if any. If the WHEN AND quals for the first action do not pass, we
 * check the second, then the third and so on. If we reach to the end, no
 * action is taken and we return true, indicating that no further action is
 * required for this tuple.
 *
 * If we do find a qualifying action, then we attempt to execute the action.
 *
 * If the tuple is concurrently updated, EvalPlanQual is run with the updated
 * tuple to recheck the join quals. Note that the additional quals associated
 * with individual actions are evaluated separately by the MERGE code, while
 * EvalPlanQual checks for the join quals. If EvalPlanQual tells us that the
 * updated tuple still passes the join quals, then we restart from the first
 * action to look for a qualifying action. Otherwise, we return false meaning
 * that a NOT MATCHED action must now be executed for the current source tuple.
 */
static bool
ExecMergeMatched(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo,
				 EState *estate, TupleTableSlot *slot,
				 ItemPointer tupleid)
{
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	bool		isNull;
	TM_FailureData tmfd;
	bool		tuple_updated,
				tuple_deleted;
	EPQState   *epqstate = &mtstate->mt_epqstate;
	ListCell   *l;

	/*
	 * If there are no WHEN MATCHED actions, we are done.
	 */
	if (resultRelInfo->ri_matchedMergeAction == NIL)
		return true;

	/*
	 * Make tuple and any needed join variables available to ExecQual and
	 * ExecProject. The target's existing tuple is installed in the scantuple.
	 * Again, this target relation's slot is required only in the case of a
	 * MATCHED tuple and UPDATE/DELETE actions.
	 */
	econtext->ecxt_scantuple = resultRelInfo->ri_oldTupleSlot;
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

lmerge_matched:

	/*
	 * UPDATE/DELETE is only invoked for matched rows. And we must have found
	 * the tupleid of the target row in that case. We fetch using SnapshotAny
	 * because we might get called again after EvalPlanQual returns us a new
	 * tuple. This tuple may not be visible to our MVCC snapshot.
	 */
	Assert(tupleid != NULL);

	if (!table_tuple_fetch_row_version(resultRelInfo->ri_RelationDesc,
									   tupleid,
									   SnapshotAny,
									   resultRelInfo->ri_oldTupleSlot))
		elog(ERROR, "failed to fetch the target tuple");

	foreach(l, resultRelInfo->ri_matchedMergeAction)
	{
		MergeActionState *relaction = (MergeActionState *) lfirst(l);
		CmdType		commandType = relaction->mas_action->commandType;

		/*
		 * Test condition, if any
		 *
		 * In the absence of a condition we perform the action unconditionally
		 * (no need to check separately since ExecQual() will return true if
		 * there are no conditions to evaluate).
		 */
		if (!ExecQual(relaction->mas_whenqual, econtext))
			continue;

		/*
		 * Check if the existing target tuple meet the USING checks of
		 * UPDATE/DELETE RLS policies. If those checks fail, we throw an
		 * error.
		 *
		 * The WITH CHECK quals are applied in ExecUpdate() and hence we need
		 * not do anything special to handle them.
		 *
		 * NOTE: We must do this after WHEN quals are evaluated so that we
		 * check policies only when they matter.
		 */
		if (resultRelInfo->ri_WithCheckOptions)
		{
			ExecWithCheckOptions(commandType == CMD_UPDATE ?
								 WCO_RLS_MERGE_UPDATE_CHECK : WCO_RLS_MERGE_DELETE_CHECK,
								 resultRelInfo,
								 resultRelInfo->ri_oldTupleSlot,
								 mtstate->ps.state);
		}

		/* Perform stated action */
		switch (commandType)
		{
			case CMD_UPDATE:

				/*
				 * We set up the projection earlier, so all we do here is
				 * Project, no need for any other tasks prior to the
				 * ExecUpdate.
				 */
				ExecProject(relaction->mas_proj);

				/*
				 * We don't call ExecFilterJunk() because the projected tuple
				 * using the UPDATE action's targetlist doesn't have a junk
				 * attribute.
				 */
				slot = ExecUpdate(mtstate, resultRelInfo,
								  tupleid, NULL,
								  resultRelInfo->ri_newTupleSlot,
								  slot, epqstate, estate,
								  &tuple_updated, &tmfd,
								  relaction, mtstate->canSetTag);
				break;

			case CMD_DELETE:
				slot = ExecDelete(mtstate, resultRelInfo,
								  tupleid, NULL,
								  slot, epqstate, estate,
								  false,
								  mtstate->canSetTag,
								  false /* changingPart */ ,
								  &tmfd,
								  relaction,
								  &tuple_deleted, NULL /* epqslot */ );

				break;

			default:
				elog(ERROR, "unknown action in MERGE WHEN MATCHED clause");

		}

		/*
		 * Check for any concurrent update/delete operation which may have
		 * prevented our update/delete. We also check for situations where we
		 * might be trying to update/delete the same tuple twice.
		 */
		if ((commandType == CMD_UPDATE && !tuple_updated) ||
			(commandType == CMD_DELETE && !tuple_deleted))
		{
			switch (tmfd.result)
			{
				case TM_Ok:
					break;
				case TM_Invisible:

					/*
					 * This state should never be reached since the underlying
					 * JOIN runs with a MVCC snapshot and EvalPlanQual runs
					 * with a dirty snapshot. So such a row should have never
					 * been returned for MERGE.
					 */
					elog(ERROR, "unexpected invisible tuple");
					break;

				case TM_SelfModified:

					/*
					 * SQLStandard disallows this for MERGE.
					 */
					if (TransactionIdIsCurrentTransactionId(tmfd.xmax))
						ereport(ERROR,
								(errcode(ERRCODE_CARDINALITY_VIOLATION),
								 errmsg("MERGE command cannot affect row a second time"),
								 errhint("Ensure that not more than one source row matches any one target row")));
					/* This shouldn't happen */
					elog(ERROR, "attempted to update or delete invisible tuple");
					break;

				case TM_Updated:
				case TM_Deleted:

					/*
					 * The target tuple was concurrently updated/deleted by
					 * some other transaction.
					 *
					 * If the current tuple is that last tuple in the update
					 * chain, then we know that the tuple was concurrently
					 * deleted. Just return and let the caller try NOT MATCHED
					 * actions.
					 *
					 * If the current tuple was concurrently updated, then we
					 * must run the EvalPlanQual() with the new version of the
					 * tuple. If EvalPlanQual() does not return a tuple then
					 * we switch to the NOT MATCHED list of actions. If it
					 * does return a tuple and the join qual is still
					 * satisfied, then we just need to recheck the MATCHED
					 * actions, starting from the top, and execute the first
					 * qualifying action.
					 */
					if (!ItemPointerEquals(tupleid, &tmfd.ctid))
					{
						Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;
						TupleTableSlot *epqslot,
									   *inputslot;
						TM_Result	result;
						int			lockmode = ExecUpdateLockMode(estate, resultRelInfo);

						inputslot = EvalPlanQualSlot(epqstate, resultRelationDesc,
													 resultRelInfo->ri_RangeTableIndex);

						result = table_tuple_lock(resultRelationDesc, tupleid,
												  estate->es_snapshot,
												  inputslot, estate->es_output_cid,
												  lockmode, LockWaitBlock,
												  TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
												  &tmfd);
						switch (result)
						{
							case TM_Ok:
								epqslot = EvalPlanQual(epqstate,
													   resultRelationDesc,
													   resultRelInfo->ri_RangeTableIndex,
													   inputslot);

								if (!TupIsNull(epqslot))
								{
									(void) ExecGetJunkAttribute(epqslot,
																resultRelInfo->ri_RowIdAttNo,
																&isNull);

									/*
									 * A non-NULL ctid means that we are still dealing
									 * with MATCHED case. But we must retry from the
									 * start with the updated tuple to ensure that the
									 * first qualifying WHEN MATCHED action is
									 * executed.
									 *
									 * We don't use the new slot returned by
									 * EvalPlanQual because we anyways re-install the
									 * new target tuple in econtext->ecxt_scantuple
									 * before re-evaluating WHEN AND conditions and
									 * re-projecting the update targetlists. The
									 * source side tuple does not change and hence we
									 * can safely continue to use the old slot.
									 */
									if (!isNull)
									{
										/*
										 * When a tuple was updated and migrate to
										 * another partition concurrently, the current
										 * MERGE implementation can't follow.  There's
										 * probably a better way to handle this case,
										 * but it'd require recognizing the relation
										 * to which the tuple moved, and setting
										 * our current resultRelInfo to that.
										 */
										if (ItemPointerIndicatesMovedPartitions(&tmfd.ctid))
											ereport(ERROR,
													(errmsg("tuple to be deleted was already moved to another partition due to concurrent update")));

										/*
										 * Must update *tupleid to the TID of the
										 * newer tuple found in the update chain.
										 */
										*tupleid = tmfd.ctid;
										slot = epqslot;
										goto lmerge_matched;
									}
								}

							case TM_Deleted:
								/* tuple already deleted; nothing to do */
								return NULL;

							case TM_SelfModified:

								/*
								 * This can be reached when following an update
								 * chain from a tuple updated by another session,
								 * reaching a tuple that was already updated in
								 * this transaction. If previously modified by
								 * this command, ignore the redundant update,
								 * otherwise error out.
								 *
								 * See also TM_SelfModified response to
								 * table_tuple_update() above.
								 */
								if (tmfd.cmax != estate->es_output_cid)
									ereport(ERROR,
											(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
											 errmsg("tuple to be updated or deleted was already modified by an operation triggered by the current command"),
											 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
									return NULL;

							default:
								/* see table_tuple_lock call in ExecDelete() */
								elog(ERROR, "unexpected table_tuple_lock status: %u",
									 result);
								return NULL;
						}

					}

					/*
					 * Tell the caller about the updated TID, restore the
					 * state back and return.
					 */
					*tupleid = tmfd.ctid;
					return false;

				default:
					break;

			}
		}

		if (commandType == CMD_UPDATE && tuple_updated)
			InstrCountFiltered2(&mtstate->ps, 1);
		if (commandType == CMD_DELETE && tuple_deleted)
			InstrCountFiltered3(&mtstate->ps, 1);

		/*
		 * We've activated one of the WHEN clauses, so we don't search
		 * further. This is required behaviour, not an optimization.
		 */
		break;
	}

	/*
	 * Successfully executed an action or no qualifying action was found.
	 */
	return true;
}

/*
 * Execute the first qualifying NOT MATCHED action.
 */
static void
ExecMergeNotMatched(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo,
					EState *estate, TupleTableSlot *slot)
{
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	ResultRelInfo *rootRelInfo = mtstate->rootResultRelInfo;
	TupleTableSlot *insert_slot;
	List	   *actionStates = NIL;
	ListCell   *l;

	/*
	 * For INSERT actions, root relation's merge action is OK since the
	 * INSERT's targetlist and the WHEN conditions can only refer to the
	 * source relation and hence it does not matter which result relation we
	 * work with.
	 *
	 * XXX does this mean that we can avoid creating copies of actionStates on
	 * partitioned tables, for not-matched actions?
	 */
	actionStates = resultRelInfo->ri_notMatchedMergeAction;

	/*
	 * Make source tuple available to ExecQual and ExecProject. We don't need
	 * the target tuple since the WHEN quals and the targetlist can't refer to
	 * the target columns.
	 */
	econtext->ecxt_scantuple = NULL;
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

	foreach(l, actionStates)
	{
		MergeActionState *action = (MergeActionState *) lfirst(l);
		CmdType		commandType = action->mas_action->commandType;

		/*
		 * Test condition, if any
		 *
		 * In the absence of a condition we perform the action unconditionally
		 * (no need to check separately since ExecQual() will return true if
		 * there are no conditions to evaluate).
		 */
		if (!ExecQual(action->mas_whenqual, econtext))
			continue;

		/* Perform stated action */
		switch (commandType)
		{
			case CMD_INSERT:
				/*
				 * Project the tuple matching the insert target table's
				 * tuple descriptor.
				 */
				insert_slot = ExecProject(action->mas_proj);

				/*
				 * If the MERGE targets a partitioned table, and the given
				 * target relation appears to be a child relation, the insert
				 * must be performed on the root relation to ensure that the
				 * inserted tuple is routed to the correct partition.
				 */
				if (resultRelInfo != rootRelInfo &&
					rootRelInfo->ri_RelationDesc->rd_rel->relkind ==
					RELKIND_PARTITIONED_TABLE);
				{
					/*
					 * The projection emitted a tuple matching the child
					 * table's descriptor, though the insert now expects to
					 * be passed to match the root partitioned table's, so
					 * convert if needed.
					 */
					TupleConversionMap *map =
						ExecGetChildToRootMap(resultRelInfo);

					if (map)
						insert_slot = execute_attr_map_slot(map->attrMap,
															insert_slot,
															mtstate->mt_root_tuple_slot);
					resultRelInfo = rootRelInfo;
				}

				(void) ExecInsert(mtstate, resultRelInfo,
								  insert_slot, slot,
								  estate, action,
								  mtstate->canSetTag);
				InstrCountFiltered1(&mtstate->ps, 1);
				break;
			case CMD_NOTHING:
				/* Do Nothing */
				break;
			default:
				elog(ERROR, "unknown action in MERGE WHEN NOT MATCHED clause");
		}

		break;
	}
}

void
ExecInitMerge(ModifyTableState *mtstate, EState *estate)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	ResultRelInfo *rootRelInfo = mtstate->rootResultRelInfo;
	ResultRelInfo *resultRelInfo;
	ExprContext *econtext;
	ListCell   *lc;
	int			i;

	if (node->mergeActionLists == NIL)
		return;

	mtstate->mt_merge_subcommands = 0;

	if (mtstate->ps.ps_ExprContext == NULL)
		ExecAssignExprContext(estate, &mtstate->ps);
	econtext = mtstate->ps.ps_ExprContext;

	/*
	 * Create a MergeActionState for each action on the mergeActionList and
	 * add it to either a list of matched actions or not-matched actions.
	 *
	 * Similar logic appears in ExecInitPartitionInfo(), so if changing
	 * anything here, do so there too.
	 */
	i = 0;
	foreach(lc, node->mergeActionLists)
	{
		List	   *mergeActionList = lfirst(lc);
		TupleDesc	relationDesc;
		ListCell   *l;

		resultRelInfo = mtstate->resultRelInfo + i;
		i++;
		relationDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

		/* initialize slot for MERGE fetches from this rel */
		if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
			ExecInitMergeProjection(mtstate, resultRelInfo);

		foreach(l, mergeActionList)
		{
			MergeAction *action = (MergeAction *) lfirst(l);
			MergeActionState *action_state;
			List	  **list;

			/*
			 * Build action merge state for this rel.  (For partitions, equivalent
			 * code exists in ExecInitPartitionInfo.)
			 */
			action_state = makeNode(MergeActionState);
			action_state->mas_action = action;
			action_state->mas_whenqual = ExecInitQual((List *) action->qual,
													   &mtstate->ps);

			/*
			 * We create two lists - one for WHEN MATCHED actions and one for WHEN
			 * NOT MATCHED actions - and stick the MergeActionState into the
			 * appropriate list.
			 */
			if (action_state->mas_action->matched)
				list = &resultRelInfo->ri_matchedMergeAction;
			else
				list = &resultRelInfo->ri_notMatchedMergeAction;
			*list = lappend(*list, action_state);

			switch (action->commandType)
			{
				case CMD_INSERT:
					ExecCheckPlanOutput(rootRelInfo->ri_RelationDesc,
										action->targetList);
					action_state->mas_proj =
						ExecBuildProjectionInfo(action->targetList, econtext,
												resultRelInfo->ri_newTupleSlot,
												&mtstate->ps,
												relationDesc);
					/*
					 * If the MERGE targets a partitioned table, any INSERT
					 * actions must be routed through it, not the child
					 * relations. Initialize the routing struct and the root
					 * table's "new" tuple slot for that, if not already
					 * done.
					 */
					if (rootRelInfo->ri_RelationDesc->rd_rel->relkind ==
						RELKIND_PARTITIONED_TABLE &&
						mtstate->mt_partition_tuple_routing == NULL)
					{
						/*
						 * Note that the slot is managed as a standalone slot
						 * belonging to ModifyTableState, so we pass NULL
						 * for the 2nd argument.
						 */
						mtstate->mt_root_tuple_slot =
							table_slot_create(rootRelInfo->ri_RelationDesc,
											  NULL);
						mtstate->mt_partition_tuple_routing =
							ExecSetupPartitionTupleRouting(estate,
														   rootRelInfo->ri_RelationDesc);
					}

					mtstate->mt_merge_subcommands |= MERGE_INSERT;
					break;
				case CMD_UPDATE:
					action_state->mas_proj =
					ExecBuildUpdateProjection(action->targetList,
											  true,
											  action->updateColnos,
											  RelationGetDescr(resultRelInfo->ri_RelationDesc),
											  econtext,
											  resultRelInfo->ri_newTupleSlot,
											  &mtstate->ps);
					mtstate->mt_merge_subcommands |= MERGE_UPDATE;
					break;
				case CMD_DELETE:
					mtstate->mt_merge_subcommands |= MERGE_DELETE;
					break;
				case CMD_NOTHING:
					break;
				default:
					elog(ERROR, "unknown operation");
					break;
			}
		}
	}
}

/*
 * Initializes the tuple slots in a ResultRelInfo for any MERGE action.
 *
 * This mimics ExecInitInsertProjection / ExecInitUpdateProjection
 */
void
ExecInitMergeProjection(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo)
{
	EState     *estate = mtstate->ps.state;

	Assert(!resultRelInfo->ri_projectNewInfoValid);

	resultRelInfo->ri_oldTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);
	resultRelInfo->ri_newTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);
	resultRelInfo->ri_projectNewInfoValid = true;
}
