#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_class.h"
#include "access/htup_details.h"

#include <sys/times.h>

PG_MODULE_MAGIC;

#define IsNewEntry(entry)	(!(entry)->object_name)

#define OBJ_KIND_FUNCTION	'F'

#define ObjIsFunction(obj)	((obj)->obj_kind == OBJ_KIND_FUNCTION)

/* 
 * Identifies a database object with a calling stored procedure context
 */
typedef struct ObjectKey 
{
	Oid			obj_id;
	Oid			calling_function_id;
} ObjectKey;

/*
 * Counters for tracking database  usage
 */
typedef struct ObjectUsageCounters
{
	PgStat_Counter		num_calls;
	PgStat_Counter		num_scans;
	PgStat_Counter		total_time;
	PgStat_Counter		self_time;
} ObjectUsageCounters;

/*
 * Essential information about the tracked object.
 */
typedef struct DatabaseObjectStats
{
	ObjectKey				key;			/* Hashing key. NB! keep as first field of the struct! */
	ObjectUsageCounters		counters;

	char					obj_kind;		/* 'F' for functions, otherwise maps to relkind (RELKIND_RELATION, etc.) */
	char				   *schema_name;
	char				   *object_name;

	/*
	 * For relations we need to keep a pointer to the stats counters.
	 * As there is no end_table_stats() function we read the counter values
	 * in report_stat() instead.
	 */
	PgStat_TableCounts	   *t_statptr;
} DatabaseObjectStats;


void _PG_init(void);
void _PG_fini(void);
static void report_stat(void);
static void start_function_stat(FunctionCallInfoData *fcinfo, PgStat_FunctionCallUsage *fcu);
static void end_function_stat(PgStat_FunctionCallUsage *fcu, bool finalize);
static void start_table_stat(Relation rel);
DatabaseObjectStats *lookup_object(Oid obj_id, Oid parent_id);

PG_FUNCTION_INFO_V1(pg_stat_usage);

static report_stat_hook_type			prev_report_stat_hook = NULL;
static start_function_stat_hook_type	prev_start_function_stat_hook = NULL;
static end_function_stat_hook_type		prev_end_function_stat_hook = NULL;
static start_table_stat_hook_type		prev_start_table_stat_hook = NULL;

static Oid 		current_function_oid = InvalidOid;
static Oid 		current_function_parent = InvalidOid;

/*
 * This holds the usage statistics for obj/calling func pairs.
 */
static HTAB    *object_usage_tab = NULL;

/*
 * We need to track the oid of current function call. Keep this in a stack until a
 * better idea comes along.
 */
static List	   *call_stack = NIL;


/*
 * Module Load Callback
 */
void
_PG_init(void)
{
	/* Install Hooks */
	prev_report_stat_hook = report_stat_hook;
	report_stat_hook = report_stat;

	prev_start_function_stat_hook = start_function_stat_hook;
	start_function_stat_hook = start_function_stat;

	prev_end_function_stat_hook = end_function_stat_hook;
	end_function_stat_hook = end_function_stat;

	prev_start_table_stat_hook = start_table_stat_hook;
	start_table_stat_hook = start_table_stat;
}

/*
 * Unload module callback.
 */
void _PG_fini(void)
{
	/*
	 * Uninstall hooks
	 */
	report_stat_hook = prev_report_stat_hook;
	start_function_stat_hook = prev_start_function_stat_hook;
	end_function_stat_hook = prev_end_function_stat_hook;
	start_table_stat_hook = prev_start_table_stat_hook;
}

/*
 * Called when the backend wants to send its accumulated stats to the collector.
 *
 * We probably need to clear the stats counter here if we're sending the stats
 * to an external collector. Make this a GUC option.
 */
static void report_stat(void)
{
	DatabaseObjectStats	   *entry;
	HASH_SEQ_STATUS			hstat;

	if (object_usage_tab)
	{
		hash_seq_init(&hstat, object_usage_tab);
		while ((entry = hash_seq_search(&hstat)) != NULL)
		{
			elog(LOG, "object usage: %s.%s oid=%u parent=%u calls=%lu scans=%lu",
					entry->schema_name,
					entry->object_name,
					entry->key.obj_id,
					entry->key.calling_function_id,
					entry->counters.num_calls,
					entry->counters.num_scans);
		}
	}
}

/*
 * Look up an object by it's oid and parent.
 *
 * Allocate the hash table if needed. Assume we're in TopMemoryContext
 *
 * Returns NULL if not found.
 */
DatabaseObjectStats *lookup_object(Oid obj_id, Oid parent_id)
{
	DatabaseObjectStats	   *entry;
	ObjectKey				key;
	bool					found;

	if (!object_usage_tab)
	{
		/* First time through, allocate the hash table */
 		HASHCTL hash_ctl;
 
 		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(key);
 		hash_ctl.entrysize = sizeof(DatabaseObjectStats);
		hash_ctl.hash = tag_hash;
 		object_usage_tab = hash_create("Object stat entries", 512, &hash_ctl, HASH_ELEM | HASH_BLOBS);
	}

	key.obj_id = obj_id;
	key.calling_function_id = parent_id;
	entry = hash_search(object_usage_tab, &key, HASH_ENTER, &found);
	Assert(entry != NULL);

	if (!found)
	{
		entry->key = key;
		memset(&entry->counters, 0, sizeof(entry->counters));
	}

	return entry;
}


/* 
 * Called when the backend starts to track the counters for a function.
 *
 * Note that this will only fire when track_function calls is set to an
 * appropriate level.
 */
static void start_function_stat(FunctionCallInfoData *fcinfo, PgStat_FunctionCallUsage *fcu)
{
	Oid					func_oid = fcinfo->flinfo->fn_oid;
	DatabaseObjectStats	*entry;
	MemoryContext 		oldctx;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	entry = lookup_object(func_oid, current_function_oid);

	if (IsNewEntry(entry))
	{
		/* 
		 * A new parent/kid combination. Set up DatabaseObjectStats for it.
		 *
		 * Note: it may be tempting to cache these results. However this is
		 * only useful when extreme number of unique lookups is performed. As
		 * per microbenchmark SearchSysCache + namespace lookup takes just
		 * about 160 clock ticks to perform 10 000 000 lookups. In comparison
		 * HTAB lookup takes 22 ticks for the same.
		 */
		Form_pg_proc functup;
		HeapTuple   tp;

		tp = SearchSysCache(PROCOID, ObjectIdGetDatum(func_oid), 0, 0, 0);
		if (!HeapTupleIsValid(tp))
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("function \"%u\" does not exist", func_oid)));

		functup = (Form_pg_proc) GETSTRUCT(tp);

		entry->object_name = pstrdup(NameStr(functup->proname));
		entry->schema_name = get_namespace_name(functup->pronamespace);
		entry->obj_kind = OBJ_KIND_FUNCTION;
 
		ReleaseSysCache(tp);
	}

	call_stack = lcons_oid(current_function_parent, call_stack);

	current_function_parent = current_function_oid;
	current_function_oid = func_oid;

	MemoryContextSwitchTo(oldctx);
}

/*
 * Note: we assume here that end_function_stat has always been preceded by
 * start_function_stat for the same function. It needs to be verified that this
 * is always the case.
 */
static void end_function_stat(PgStat_FunctionCallUsage *fcu, bool finalize)
{
	ObjectKey				key;
	DatabaseObjectStats	   *entry;

	/* Could happen if the library is loaded from a stored procedure. */
	if (!object_usage_tab || !call_stack)
		return;

	key.obj_id = current_function_oid;
	key.calling_function_id = current_function_parent;

	entry = hash_search(object_usage_tab, &key, HASH_FIND, NULL);
	Assert(entry);

	if (finalize)
		entry->counters.num_calls++;

	/* We need to "pop" the current function oid and parent */
	current_function_oid = key.calling_function_id;
	current_function_parent = linitial_oid(call_stack);

	call_stack = list_delete_first(call_stack);
}

/*
 * Called when the backend wants to add some stats to a relation.
 *
 * We only try to look at non-system objects.
 */
static void start_table_stat(Relation rel)
{
	DatabaseObjectStats	*entry;
	MemoryContext 		oldctx;

	if (!rel->pgstat_info)
		return;

	if (rel->rd_id < FirstNormalObjectId)
		/* Skip system objects for now */
		return;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	entry = lookup_object(rel->rd_id, current_function_oid);

	if (IsNewEntry(entry))
	{
		Form_pg_class 		reltup;
		HeapTuple			tp;

		tp = SearchSysCache(RELOID, ObjectIdGetDatum(rel->rd_id), 0, 0, 0);
	
		if (!HeapTupleIsValid(tp))
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("relation \"%u\" does not exist", rel->rd_id)));
	
		reltup = (Form_pg_class) GETSTRUCT(tp);
	
		entry->object_name = pstrdup(NameStr(reltup->relname));
		entry->schema_name = get_namespace_name(reltup->relnamespace);
		entry->obj_kind = reltup->relkind;

		ReleaseSysCache(tp);
	} 

	entry->t_statptr = &rel->pgstat_info->t_counts;

	MemoryContextSwitchTo(oldctx);
}

/*
 * Fetch usage stats
 */
Datum
pg_stat_usage(PG_FUNCTION_ARGS)
{
	ReturnSetInfo			   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc					tupdesc;
	Tuplestorestate			   *tupstore;
	MemoryContext 				per_query_ctx;
	MemoryContext 				oldcontext;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (object_usage_tab)
	{
		HASH_SEQ_STATUS			hstat;
		DatabaseObjectStats	   *entry;
		ObjectUsageCounters		all_zeroes;

		memset(&all_zeroes, 0, sizeof(all_zeroes));

		hash_seq_init(&hstat, object_usage_tab);
		while ((entry = hash_seq_search(&hstat)) != NULL)
		{
			Datum		values[6];
			bool		nulls[6];
			char		buf[2];
			int			i = 0;

			/* Skip objects with no stats */
			if (memcmp(&entry->counters, &all_zeroes, sizeof(all_zeroes)) == 0)
				continue;

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));
			buf[0] = entry->obj_kind;
			buf[1] = '\0';

			values[i++] = ObjectIdGetDatum(entry->key.obj_id);
			values[i++] = ObjectIdGetDatum(entry->key.calling_function_id);
			values[i++] = CStringGetTextDatum(buf);
			values[i++] = CStringGetTextDatum(entry->schema_name);
			values[i++] = CStringGetTextDatum(entry->object_name);
			values[i++] = Int64GetDatumFast(entry->counters.num_calls);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

