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
#include "access/htup_details.h"

#include <sys/times.h>

PG_MODULE_MAGIC;

/* 
 * Key into parent/child function call hash table
 */
typedef struct FunctionUsageKey 
{
	Oid			f_kid;
	Oid			f_parent;
} FunctionUsageKey;

/*
 * Counters for tracking function usage
 */
typedef struct FunctionUsageCounters
{
	PgStat_Counter		num_calls;
	PgStat_Counter		total_time;
	PgStat_Counter		self_time;
} FunctionUsageCounters;

/*
 * Essential information about the tracked functions.
 */
typedef struct BackendFunctionCallInfo
{
	FunctionUsageKey		f_key;
	FunctionUsageCounters	f_counters;
	int						f_nargs;
	char				   *f_schema;
	char				   *f_name;
} BackendFunctionCallInfo;

/* 
 * Key into table/funccontext hash table
 */
typedef struct TableUsageKey
{
	Oid						t_id;
	Oid						t_func_context;
} TableUsageKey;

/*
 * Track table usage counters. Basically same as Relation structure has, but also
 * keep function call context (if there is any).
 */
typedef struct TableUsageInfo
{
	TableUsageKey			t_key;
	char				   *t_name;
	char				   *t_schema;
	char				 	t_relkind;
	PgStat_TableCounts	   *t_statptr;
	PgStat_TableCounts	    t_counters;
} TableUsageInfo;

void _PG_init(void);
void _PG_fini(void);
static void report_stat(void);
static void start_function_stat(FunctionCallInfoData *fcinfo, PgStat_FunctionCallUsage *fcu);
static void end_function_stat(PgStat_FunctionCallUsage *fcu, bool finalize);
static void start_table_stat(Relation rel);

PG_FUNCTION_INFO_V1(pg_stat_usage);

static report_stat_hook_type			prev_report_stat_hook = NULL;
static start_function_stat_hook_type	prev_start_function_stat_hook = NULL;
static end_function_stat_hook_type		prev_end_function_stat_hook = NULL;
static start_table_stat_hook_type		prev_start_table_stat_hook = NULL;

static Oid 		current_function_oid = InvalidOid;
static Oid 		current_function_parent = InvalidOid;

/*
 * This holds the call statistics for parent/func pairs.
 */
static HTAB    *function_usage_tab = NULL;

/*
 * This is for the relation's stats
 */
static HTAB	   *relation_usage_tab = NULL;

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
 * Called, when the backend wants to send its accumulated stats to the collector.
 *
 * NB! This is performed outside of a transaction. No syscache lookups here.
 */
static void report_stat(void)
{
	BackendFunctionCallInfo	   *bfci;
	TableUsageInfo			   *tui;
	HASH_SEQ_STATUS				hstat;

	if (function_usage_tab)
	{
		hash_seq_init(&hstat, function_usage_tab);
		while ((bfci = hash_seq_search(&hstat)) != NULL)
		{
			elog(LOG, "function call: %s.%s(%d) oid=%u parent=%u calls=%lu",
					bfci->f_schema,
					bfci->f_name,
					bfci->f_nargs,
					bfci->f_key.f_kid,
					bfci->f_key.f_parent,
					bfci->f_counters.num_calls);
		}
	}

	if (relation_usage_tab)
	{
		hash_seq_init(&hstat, relation_usage_tab);
		while ((tui = hash_seq_search(&hstat)) != NULL)
		{
			if (tui->t_statptr)
			{
				/* Make a copy of the counters so that they don't get lost during stats report */
				tui->t_counters.t_numscans += tui->t_statptr->t_numscans;
				elog(LOG, "table access: %s.%s oid=%u function=%u scans=%lu",
					tui->t_schema, tui->t_name, tui->t_key.t_id, tui->t_key.t_func_context,
					tui->t_counters.t_numscans);
			}
		}
	}
}

/* 
 * Called when the backend starts to track the counters for a function.
 *
 * Note that this will only fire when track_function calls is set to an
 * appropriate level.
 */
static void start_function_stat(FunctionCallInfoData *fcinfo, PgStat_FunctionCallUsage *fcu)
{
	FunctionUsageKey	key;
	Oid					func_oid = fcinfo->flinfo->fn_oid;
	MemoryContext 		oldctx;
	BackendFunctionCallInfo	*bfci;
	bool				found;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	key.f_kid = func_oid;
	key.f_parent = current_function_oid;

	if (!function_usage_tab)
	{
		/* First time through, set up the functions hash table */
 		HASHCTL hash_ctl;
 
 		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(key);
 		hash_ctl.entrysize = sizeof(BackendFunctionCallInfo);
		hash_ctl.hash = tag_hash;
 		function_usage_tab = hash_create("Function stat entries", 512, &hash_ctl, HASH_ELEM | HASH_BLOBS);
	}

	bfci = hash_search(function_usage_tab, &key, HASH_ENTER, &found);

	if (!found)
	{
		/* 
		 * A new parent/kid combination. Set up BackendFunctionCallInfo for it.
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

		bfci->f_key = key;
		bfci->f_name = pstrdup(NameStr(functup->proname));
		bfci->f_schema = get_namespace_name(functup->pronamespace);
		bfci->f_nargs = functup->pronargs;
		MemSet(&bfci->f_counters, 0, sizeof(bfci->f_counters));
 
		ReleaseSysCache(tp);
	}

	bfci->f_counters.num_calls++;

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
	MemoryContext 			oldctx;
	FunctionUsageKey		key;
	BackendFunctionCallInfo	*bfci;
	bool					found;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	key.f_kid = current_function_oid;
	key.f_parent = current_function_parent;

	bfci = hash_search(function_usage_tab, &key, HASH_FIND, &found);
	Assert(found);
	Assert(bfci);

	/* We need to "pop" the current function oid and parent */
	current_function_oid = key.f_parent;
	current_function_parent = linitial_oid(call_stack);

	call_stack = list_delete_first(call_stack);

	MemoryContextSwitchTo(oldctx);
}

/*
 * Called when the backend wants to add some stats to a relation.
 *
 * We only try to look at non-system objects.
 */
static void start_table_stat(Relation rel)
{
	TableUsageKey		key;
	MemoryContext 		oldctx;
	TableUsageInfo	   *tui;
	bool				found;

	if (!rel->pgstat_info)
		return;

	if (rel->rd_id < FirstNormalObjectId)
		/* Skip system objects for now */
		return;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	key.t_id = rel->rd_id;
	key.t_func_context = current_function_oid;

	if (!relation_usage_tab)
	{
		/* First time through, set up the functions hash table */
 		HASHCTL hash_ctl;
 
 		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(key);
 		hash_ctl.entrysize = sizeof(TableUsageInfo);
		hash_ctl.hash = tag_hash;
 		relation_usage_tab = hash_create("Table stat entries", 512, &hash_ctl, HASH_ELEM | HASH_BLOBS);
	}

	tui = hash_search(relation_usage_tab, &key, HASH_ENTER, &found);

	if (!found)
	{
		Form_pg_class 		reltup;
		HeapTuple			tp;

		tp = SearchSysCache(RELOID, ObjectIdGetDatum(rel->rd_id), 0, 0, 0);
	
		if (!HeapTupleIsValid(tp))
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("relation \"%u\" does not exist", rel->rd_id)));
	
		reltup = (Form_pg_class) GETSTRUCT(tp);
	
		tui->t_name = pstrdup(NameStr(reltup->relname));
		tui->t_schema = get_namespace_name(reltup->relnamespace);
		tui->t_relkind = reltup->relkind;

		MemSet(&tui->t_counters, 0, sizeof(tui->t_counters));
		ReleaseSysCache(tp);
	} 

	tui->t_statptr = &rel->pgstat_info->t_counts;

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

	if (function_usage_tab)
	{
		HASH_SEQ_STATUS 			hstat;
		BackendFunctionCallInfo	   *entry;

		hash_seq_init(&hstat, function_usage_tab);
		while ((entry = hash_seq_search(&hstat)) != NULL)
		{
			Datum		values[6];
			bool		nulls[6];
			int			i = 0;

			if (!entry->f_counters.num_calls)
				continue;

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			values[i++] = ObjectIdGetDatum(entry->f_key.f_kid);
			values[i++] = ObjectIdGetDatum(entry->f_key.f_parent);
			values[i++] = CStringGetTextDatum("F");
			values[i++] = CStringGetTextDatum(entry->f_schema);
			values[i++] = CStringGetTextDatum(entry->f_name);
			values[i++] = Int64GetDatumFast(entry->f_counters.num_calls);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	if (relation_usage_tab)
	{
		HASH_SEQ_STATUS			hstat;
		TableUsageInfo		   *entry;

		hash_seq_init(&hstat, relation_usage_tab);
		while ((entry = hash_seq_search(&hstat)) != NULL)
		{
			Datum		values[6];
			bool		nulls[6];
			int			i = 0;
			char		buf[2];

			if (!entry->t_counters.t_numscans)
				continue;

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			buf[0] = entry->t_relkind;
			buf[1] = '\0';

			values[i++] = ObjectIdGetDatum(entry->t_key.t_id);
			values[i++] = ObjectIdGetDatum(entry->t_key.t_func_context);
			values[i++] = CStringGetTextDatum(buf);
			values[i++] = CStringGetTextDatum(entry->t_schema);
			values[i++] = CStringGetTextDatum(entry->t_name);
			values[i++] = Int64GetDatumFast(entry->t_counters.t_numscans);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}

