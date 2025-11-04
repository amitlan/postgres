/*-------------------------------------------------------------------------
 *
 * pgpa_collector.c
 *	  collect advice into backend-local or shared memory
 *
 * Copyright (c) 2016-2025, PostgreSQL Global Development Group
 *
 *	  contrib/pg_plan_advice/pgpa_collector.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pg_plan_advice.h"
#include "pgpa_collector.h"

#include "datatype/timestamp.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

PG_FUNCTION_INFO_V1(pg_clear_collected_local_advice);
PG_FUNCTION_INFO_V1(pg_clear_collected_shared_advice);
PG_FUNCTION_INFO_V1(pg_get_collected_local_advice);
PG_FUNCTION_INFO_V1(pg_get_collected_shared_advice);

#define ADVICE_CHUNK_SIZE		1024
#define ADVICE_CHUNK_ARRAY_SIZE	64

#define	PG_GET_ADVICE_COLUMNS	7

/*
 * Advice extracted from one query plan, together with the query string
 * and various other identifying details.
 */
typedef struct pgpa_collected_advice
{
	Oid			userid;			/* user OID */
	Oid			dbid;			/* database OID */
	uint64		queryid;		/* query identifier */
	TimestampTz timestamp;		/* query timestamp */
	int			advice_offset;	/* start of advice in textual data */
	char		textual_data[FLEXIBLE_ARRAY_MEMBER];
} pgpa_collected_advice;

/*
 * A bunch of pointers to pgpa_collected_advice objects, stored in
 * backend-local memory.
 */
typedef struct pgpa_local_advice_chunk
{
	pgpa_collected_advice *entries[ADVICE_CHUNK_SIZE];
} pgpa_local_advice_chunk;

/*
 * Information about all of the pgpa_collected_advice objects that we're
 * storing in local memory.
 *
 * We assign consecutive IDs, starting from 0, to each pgpa_collected_advice
 * object that we store. The actual storage is an array of chunks, which
 * helps keep memcpy() overhead low when we start discarding older data.
 */
typedef struct pgpa_local_advice
{
	uint64		next_id;
	uint64		oldest_id;
	uint64		base_id;
	int			chunk_array_allocated_size;
	pgpa_local_advice_chunk **chunks;
} pgpa_local_advice;

/*
 * Just like pgpa_local_advice_chunk, but stored in a dynamic shared area,
 * so we must use dsa_pointer instead of native pointers.
 */
typedef struct pgpa_shared_advice_chunk
{
	dsa_pointer entries[ADVICE_CHUNK_SIZE];
} pgpa_shared_advice_chunk;

/*
 * Just like pgpa_local_advice, but stored in a dynamic shared area, so
 * we must use dsa_pointer instead of native pointers.
 */
typedef struct pgpa_shared_advice
{
	uint64		next_id;
	uint64		oldest_id;
	uint64		base_id;
	int			chunk_array_allocated_size;
	dsa_pointer chunks;
} pgpa_shared_advice;

/* Pointers to local and shared collectors */
static pgpa_local_advice *local_collector = NULL;
static pgpa_shared_advice *shared_collector = NULL;

/* Static functions */
static pgpa_collected_advice *pgpa_make_collected_advice(Oid userid,
														 Oid dbid,
														 uint64 queryId,
														 TimestampTz timestamp,
														 const char *query_string,
														 const char *advice_string,
														 dsa_area *area,
														 dsa_pointer *result);
static void pgpa_store_local_advice(pgpa_collected_advice *ca);
static void pgpa_trim_local_advice(int limit);
static void pgpa_store_shared_advice(dsa_pointer ca_pointer);
static void pgpa_trim_shared_advice(dsa_area *area, int limit);

/* Helper function to extract the query string from pgpa_collected_advice */
static inline const char *
query_string(pgpa_collected_advice *ca)
{
	return ca->textual_data;
}

/* Helper function to extract the advice string from pgpa_collected_advice */
static inline const char *
advice_string(pgpa_collected_advice *ca)
{
	return ca->textual_data + ca->advice_offset;
}

/*
 * Store collected query advice into the local or shared advice collector,
 * as appropriate.
 */
void
pgpa_collect_advice(uint64 queryId, const char *query_string,
					const char *advice_string)
{
	Oid			userid = GetUserId();
	Oid			dbid = MyDatabaseId;
	TimestampTz now = GetCurrentTimestamp();

	if (pg_plan_advice_local_collection_limit > 0)
	{
		pgpa_collected_advice *ca;
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(pg_plan_advice_get_mcxt());
		ca = pgpa_make_collected_advice(userid, dbid, queryId, now,
										query_string, advice_string,
										NULL, NULL);
		pgpa_store_local_advice(ca);
		MemoryContextSwitchTo(oldcontext);
	}

	if (pg_plan_advice_shared_collection_limit > 0)
	{
		dsa_area   *area = pg_plan_advice_dsa_area();
		dsa_pointer ca_pointer;

		pgpa_make_collected_advice(userid, dbid, queryId, now,
								   query_string, advice_string, area,
								   &ca_pointer);
		pgpa_store_shared_advice(ca_pointer);
	}
}

/*
 * Allocate and fill a new pgpa_collected_advice object.
 *
 * If area != NULL, it is used to allocate the new object, and the resulting
 * dsa_pointer is returned via *result.
 *
 * If area == NULL, the new object is allocated in the current memory context,
 * and result is not examined or modified.
 */
static pgpa_collected_advice *
pgpa_make_collected_advice(Oid userid, Oid dbid, uint64 queryId,
						   TimestampTz timestamp,
						   const char *query_string,
						   const char *advice_string,
						   dsa_area *area, dsa_pointer *result)
{
	size_t		query_string_length = strlen(query_string) + 1;
	size_t		advice_string_length = strlen(advice_string) + 1;
	size_t		total_length;
	pgpa_collected_advice *ca;

	total_length = offsetof(pgpa_collected_advice, textual_data)
		+ query_string_length + advice_string_length;

	if (area == NULL)
		ca = palloc(total_length);
	else
	{
		*result = dsa_allocate(area, total_length);
		ca = dsa_get_address(area, *result);
	}

	ca->userid = GetUserId();
	ca->dbid = MyDatabaseId;
	ca->queryid = queryId;
	ca->timestamp = timestamp;
	ca->advice_offset = query_string_length;

	memcpy(ca->textual_data, query_string, query_string_length);
	memcpy(&ca->textual_data[ca->advice_offset],
		   advice_string, advice_string_length);

	return ca;
}

/*
 * Add a pg_collected_advice object to our backend-local advice collection.
 *
 * Caller is responsible for switching to the appropriate memory context;
 * the provided object should have been allocated in that same context.
 */
static void
pgpa_store_local_advice(pgpa_collected_advice *ca)
{
	uint64		chunk_number;
	uint64		chunk_offset;
	pgpa_local_advice *la = local_collector;

	/* If the local advice collector isn't initialized yet, do that now. */
	if (la == NULL)
	{
		la = palloc0(sizeof(pgpa_local_advice));
		la->chunk_array_allocated_size = ADVICE_CHUNK_ARRAY_SIZE;
		la->chunks = palloc0_array(pgpa_local_advice_chunk *,
								   la->chunk_array_allocated_size);
		local_collector = la;
	}

	/* Compute chunk and offset at which to store this advice. */
	chunk_number = (la->next_id - la->base_id) / ADVICE_CHUNK_SIZE;
	chunk_offset = (la->next_id - la->base_id) % ADVICE_CHUNK_SIZE;

	/* Extend chunk array, if needed. */
	if (chunk_number >= la->chunk_array_allocated_size)
	{
		int			new_size;

		new_size = la->chunk_array_allocated_size + ADVICE_CHUNK_ARRAY_SIZE;
		la->chunks = repalloc0_array(la->chunks,
									 pgpa_local_advice_chunk *,
									 la->chunk_array_allocated_size,
									 new_size);
		la->chunk_array_allocated_size = new_size;
	}

	/* Allocate new chunk, if needed. */
	if (la->chunks[chunk_number] == NULL)
		la->chunks[chunk_number] = palloc0_object(pgpa_local_advice_chunk);

	/* Save pointer and bump next-id counter. */
	Assert(la->chunks[chunk_number]->entries[chunk_offset] == NULL);
	la->chunks[chunk_number]->entries[chunk_offset] = ca;
	++la->next_id;

	/* If we've exceeded the storage limit, discard old data. */
	pgpa_trim_local_advice(pg_plan_advice_local_collection_limit);
}

/*
 * Add a pg_collected_advice object to the shared advice collection.
 *
 * 'ca_pointer' should have been allocated from the pg_plan_advice DSA area
 * and should point to an object of type pgpa_collected_advice.
 */
static void
pgpa_store_shared_advice(dsa_pointer ca_pointer)
{
	uint64		chunk_number;
	uint64		chunk_offset;
	pgpa_shared_state *state = pg_plan_advice_attach();
	dsa_area   *area = pg_plan_advice_dsa_area();
	pgpa_shared_advice *sa = shared_collector;
	dsa_pointer *chunk_array;
	pgpa_shared_advice_chunk *chunk;

	/* Lock the shared state. */
	LWLockAcquire(&state->lock, LW_EXCLUSIVE);

	/*
	 * If we're not attached to the shared advice collector yet, fix that now.
	 * If we're the first ones to attach, we may need to create the object.
	 */
	if (sa == NULL)
	{
		if (state->shared_collector == InvalidDsaPointer)
			state->shared_collector =
				dsa_allocate0(area, sizeof(pgpa_shared_advice));
		shared_collector = sa = dsa_get_address(area, state->shared_collector);
	}

	/*
	 * It's possible that some other backend may have succeeded in creating
	 * the main collector object but failed to allocate an initial chunk
	 * array, so we must be prepared to allocate the chunk array here whether
	 * or not we created the collector object.
	 */
	if (shared_collector->chunk_array_allocated_size == 0)
	{
		sa->chunks =
			dsa_allocate0(area,
						  sizeof(dsa_pointer) * ADVICE_CHUNK_ARRAY_SIZE);
		sa->chunk_array_allocated_size = ADVICE_CHUNK_ARRAY_SIZE;
	}

	/* Compute chunk and offset at which to store this advice. */
	chunk_number = (sa->next_id - sa->base_id) / ADVICE_CHUNK_SIZE;
	chunk_offset = (sa->next_id - sa->base_id) % ADVICE_CHUNK_SIZE;

	/* Get the address of the chunk array and, if needed, extend it. */
	if (chunk_number >= sa->chunk_array_allocated_size)
	{
		int			new_size;
		dsa_pointer new_chunks;

		/*
		 * DSA can't enlarge an existing allocation, so we must make a new
		 * allocation and copy data over.
		 */
		new_size = sa->chunk_array_allocated_size + ADVICE_CHUNK_ARRAY_SIZE;
		new_chunks = dsa_allocate0(area, sizeof(dsa_pointer) * new_size);
		chunk_array = dsa_get_address(area, new_chunks);
		memcpy(chunk_array, dsa_get_address(area, sa->chunks),
			   sizeof(dsa_pointer) * sa->chunk_array_allocated_size);
		dsa_free(area, sa->chunks);
		sa->chunks = new_chunks;
		sa->chunk_array_allocated_size = new_size;
	}
	else
		chunk_array = dsa_get_address(area, sa->chunks);

	/* Get the address of the desired chunk, allocating it if needed. */
	if (chunk_array[chunk_number] == InvalidDsaPointer)
		chunk_array[chunk_number] =
			dsa_allocate0(area, sizeof(pgpa_shared_advice_chunk));
	chunk = dsa_get_address(area, chunk_array[chunk_number]);

	/* Save pointer and bump next-id counter. */
	Assert(chunk->entries[chunk_offset] == InvalidDsaPointer);
	chunk->entries[chunk_offset] = ca_pointer;
	++sa->next_id;

	/* If we've exceeded the storage limit, discard old data. */
	pgpa_trim_shared_advice(area, pg_plan_advice_shared_collection_limit);

	/* Release lock on shared state. */
	LWLockRelease(&state->lock);
}

/*
 * Discard collected advice stored in backend-local memory in excess of the
 * specified limit.
 */
static void
pgpa_trim_local_advice(int limit)
{
	pgpa_local_advice *la = local_collector;
	uint64		current_count;
	uint64		trim_count;
	uint64		total_chunk_count;
	uint64		trim_chunk_count;
	uint64		remaining_chunk_count;

	/* If we haven't yet reached the limit, there's nothing to do. */
	current_count = la->next_id - la->oldest_id;
	if (current_count <= limit)
		return;

	/* Free enough entries to get us back down to the limit. */
	trim_count = current_count - limit;
	while (trim_count > 0)
	{
		uint64		chunk_number;
		uint64		chunk_offset;

		chunk_number = (la->oldest_id - la->base_id) / ADVICE_CHUNK_SIZE;
		chunk_offset = (la->oldest_id - la->base_id) % ADVICE_CHUNK_SIZE;

		Assert(la->chunks[chunk_number]->entries[chunk_offset] != NULL);
		pfree(la->chunks[chunk_number]->entries[chunk_offset]);
		la->chunks[chunk_number]->entries[chunk_offset] = NULL;
		++la->oldest_id;
		--trim_count;
	}

	/* Free any chunks that are now entirely unused. */
	trim_chunk_count = (la->oldest_id - la->base_id) / ADVICE_CHUNK_SIZE;
	for (uint64 n = 0; n < trim_chunk_count; ++n)
		pfree(la->chunks[n]);

	/* Slide remaining chunk pointers back toward the base of the array. */
	total_chunk_count = (la->next_id - la->base_id +
						 ADVICE_CHUNK_SIZE - 1) / ADVICE_CHUNK_SIZE;
	remaining_chunk_count = total_chunk_count - trim_chunk_count;
	if (remaining_chunk_count > 0)
		memmove(&la->chunks[0], &la->chunks[trim_chunk_count],
				sizeof(pgpa_local_advice_chunk *) * remaining_chunk_count);

	/* Don't leave stale pointers around. */
	memset(&la->chunks[remaining_chunk_count], 0,
		   sizeof(pgpa_local_advice_chunk *)
		   * (total_chunk_count - remaining_chunk_count));

	/* Adjust base ID value accordingly. */
	la->base_id += trim_chunk_count * ADVICE_CHUNK_SIZE;
}

/*
 * Discard collected advice stored in shared memory in excess of the
 * specified limit.
 */
static void
pgpa_trim_shared_advice(dsa_area *area, int limit)
{
	pgpa_shared_advice *sa = shared_collector;
	uint64		current_count;
	uint64		trim_count;
	uint64		total_chunk_count;
	uint64		trim_chunk_count;
	uint64		remaining_chunk_count;
	dsa_pointer *chunk_array;

	/* If we haven't yet reached the limit, there's nothing to do. */
	current_count = sa->next_id - sa->oldest_id;
	if (current_count <= limit)
		return;

	/* Get a pointer to the chunk array. */
	chunk_array = dsa_get_address(area, sa->chunks);

	/* Free enough entries to get us back down to the limit. */
	trim_count = current_count - limit;
	while (trim_count > 0)
	{
		uint64		chunk_number;
		uint64		chunk_offset;
		pgpa_shared_advice_chunk *chunk;

		chunk_number = (sa->oldest_id - sa->base_id) / ADVICE_CHUNK_SIZE;
		chunk_offset = (sa->oldest_id - sa->base_id) % ADVICE_CHUNK_SIZE;

		chunk = dsa_get_address(area, chunk_array[chunk_number]);
		Assert(chunk->entries[chunk_offset] != InvalidDsaPointer);
		dsa_free(area, chunk->entries[chunk_offset]);
		chunk->entries[chunk_offset] = InvalidDsaPointer;
		++sa->oldest_id;
		--trim_count;
	}

	/* Free any chunks that are now entirely unused. */
	trim_chunk_count = (sa->oldest_id - sa->base_id) / ADVICE_CHUNK_SIZE;
	for (uint64 n = 0; n < trim_chunk_count; ++n)
		dsa_free(area, chunk_array[n]);

	/* Slide remaining chunk pointers back toward the base of the array. */
	total_chunk_count = (sa->next_id - sa->base_id +
						 ADVICE_CHUNK_SIZE - 1) / ADVICE_CHUNK_SIZE;
	remaining_chunk_count = total_chunk_count - trim_chunk_count;
	if (remaining_chunk_count > 0)
		memmove(&chunk_array[0], &chunk_array[trim_chunk_count],
				sizeof(dsa_pointer) * remaining_chunk_count);

	/* Don't leave stale pointers around. */
	memset(&chunk_array[remaining_chunk_count], 0,
		   sizeof(pgpa_shared_advice_chunk *)
		   * (total_chunk_count - remaining_chunk_count));

	/* Adjust base ID value accordingly. */
	sa->base_id += trim_chunk_count * ADVICE_CHUNK_SIZE;
}

/*
 * SQL-callable function to discard advice collected in backend-local memory
 */
Datum
pg_clear_collected_local_advice(PG_FUNCTION_ARGS)
{
	if (local_collector != NULL)
		pgpa_trim_local_advice(0);

	PG_RETURN_VOID();
}

/*
 * SQL-callable function to discard advice collected in backend-local memory
 */
Datum
pg_clear_collected_shared_advice(PG_FUNCTION_ARGS)
{
	pgpa_shared_state *state = pg_plan_advice_attach();
	dsa_area   *area = pg_plan_advice_dsa_area();

	LWLockAcquire(&state->lock, LW_EXCLUSIVE);

	/*
	 * If we're not attached to the shared advice collector yet, fix that now;
	 * but if the collector doesn't even exist, we can return without doing
	 * anything else.
	 */
	if (shared_collector == NULL)
	{
		if (state->shared_collector == InvalidDsaPointer)
		{
			LWLockRelease(&state->lock);
			return (Datum) 0;
		}
		shared_collector = dsa_get_address(area, state->shared_collector);
	}

	/* Do the real work */
	pgpa_trim_shared_advice(area, 0);

	LWLockRelease(&state->lock);

	PG_RETURN_VOID();
}

/*
 * SQL-callable SRF to return advice collected in backend-local memory
 */
Datum
pg_get_collected_local_advice(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pgpa_local_advice *la = local_collector;
	Oid			userid = GetUserId();

	InitMaterializedSRF(fcinfo, 0);

	if (la == NULL)
		return (Datum) 0;

	/* Loop over all entries. */
	for (uint64 id = la->oldest_id; id < la->next_id; ++id)
	{
		uint64		chunk_number;
		uint64		chunk_offset;
		pgpa_collected_advice *ca;
		Datum		values[PG_GET_ADVICE_COLUMNS];
		bool		nulls[PG_GET_ADVICE_COLUMNS] = {0};

		chunk_number = (id - la->base_id) / ADVICE_CHUNK_SIZE;
		chunk_offset = (id - la->base_id) % ADVICE_CHUNK_SIZE;

		ca = la->chunks[chunk_number]->entries[chunk_offset];

		if (!member_can_set_role(userid, ca->userid))
			continue;

		values[0] = UInt64GetDatum(id);
		values[1] = ObjectIdGetDatum(ca->userid);
		values[2] = ObjectIdGetDatum(ca->dbid);
		values[3] = UInt64GetDatum(ca->queryid);
		values[4] = TimestampGetDatum(ca->timestamp);
		values[5] = CStringGetTextDatum(query_string(ca));
		values[6] = CStringGetTextDatum(advice_string(ca));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}

/*
 * SQL-callable SRF to return advice collected in shared memory
 */
Datum
pg_get_collected_shared_advice(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pgpa_shared_state *state = pg_plan_advice_attach();
	dsa_area   *area = pg_plan_advice_dsa_area();
	dsa_pointer *chunk_array;
	pgpa_shared_advice *sa = shared_collector;

	InitMaterializedSRF(fcinfo, 0);

	/* Lock the shared state. */
	LWLockAcquire(&state->lock, LW_SHARED);

	/*
	 * If we're not attached to the shared advice collector yet, fix that now;
	 * but if the collector doesn't even exist, we can return without doing
	 * anything else.
	 */
	if (sa == NULL)
	{
		if (state->shared_collector == InvalidDsaPointer)
		{
			LWLockRelease(&state->lock);
			return (Datum) 0;
		}
		shared_collector = sa = dsa_get_address(area, state->shared_collector);
	}

	/* Get a pointer to the chunk array. */
	chunk_array = dsa_get_address(area, sa->chunks);

	/* Loop over all entries. */
	for (uint64 id = sa->oldest_id; id < sa->next_id; ++id)
	{
		uint64		chunk_number;
		uint64		chunk_offset;
		pgpa_shared_advice_chunk *chunk;
		pgpa_collected_advice *ca;
		Datum		values[PG_GET_ADVICE_COLUMNS];
		bool		nulls[PG_GET_ADVICE_COLUMNS] = {0};

		chunk_number = (id - sa->base_id) / ADVICE_CHUNK_SIZE;
		chunk_offset = (id - sa->base_id) % ADVICE_CHUNK_SIZE;

		chunk = dsa_get_address(area, chunk_array[chunk_number]);
		ca = dsa_get_address(area, chunk->entries[chunk_offset]);

		values[0] = UInt64GetDatum(id);
		values[1] = ObjectIdGetDatum(ca->userid);
		values[2] = ObjectIdGetDatum(ca->dbid);
		values[3] = UInt64GetDatum(ca->queryid);
		values[4] = TimestampGetDatum(ca->timestamp);
		values[5] = CStringGetTextDatum(query_string(ca));
		values[6] = CStringGetTextDatum(advice_string(ca));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	/* Release lock on shared state. */
	LWLockRelease(&state->lock);

	return (Datum) 0;
}
