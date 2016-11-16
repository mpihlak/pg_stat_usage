#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_proc.h"
#include "access/htup_details.h"


PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

static void report_stat(void);
static void start_function_stat(FunctionCallInfoData *fcinfo, PgStat_FunctionCallUsage *fcu);
static void end_function_stat(PgStat_FunctionCallUsage *fcu, bool finalize);
static void start_table_stat(Relation rel);

static report_stat_hook_type			prev_report_stat_hook = NULL;
static start_function_stat_hook_type	prev_start_function_stat_hook = NULL;
static end_function_stat_hook_type		prev_end_function_stat_hook = NULL;
static start_table_stat_hook_type		prev_start_table_stat_hook = NULL;

static bool 	track_function_calls = false;
static bool 	track_call_graph = false;
static bool 	track_table_stats = false;

static Oid 		current_function_oid = InvalidOid;
static Oid 		current_function_parent = InvalidOid;
static int		current_depth = 0;

static HTAB    *function_usage_tab = NULL;

/*
 * We need to track the oid of current function call. Keep this in a stack until a
 * better idea comes along.
 */
static List	   *call_stack = NIL;

/* 
 * Key into parent/child function call hash table
 */
typedef struct FunctionUsageKey 
{
	Oid			f_kid;
	Oid			f_parent;
} FunctionUsageKey;

/*
 * Essential information about the tracked functions.
 */
typedef struct BackendFunctionCallInfo
{
	FunctionUsageKey	f_key;
	int					f_nargs;
	char			   *f_schema;
	char			   *f_name;
} BackendFunctionCallInfo;

/*
 * Module Load Callback
 */
void
_PG_init(void)
{
	DefineCustomBoolVariable("pg_stat_usage.track_function_calls",
			"Selects whether function calls are tracked by pg_stat_usage",
			NULL,
			&track_function_calls,
			true,
			PGC_SUSET,
			0,
			NULL,
			NULL,
			NULL);

	DefineCustomBoolVariable("pg_stat_usage.track_call_graph",
			"Selects whether function call graph is tracked by pg_stat_usage",
			NULL,
			&track_call_graph,
			true,
			PGC_SUSET,
			0,
			NULL,
			NULL,
			NULL);

	DefineCustomBoolVariable("pg_stat_usage.track_table_stats",
			"Selects whether table and index stats are tracked by pg_stat_usage",
			NULL,
			&track_table_stats,
			true,
			PGC_SUSET,
			0,
			NULL,
			NULL,
			NULL);

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

static void report_stat(void)
{
}

/* 
 * Register the function in our internal tables
 * Look up the name of the function (local cache and syscache, if not local)
 * Track parent/child relationships
 *
 * Note that this will only fire when track_function calls is set to an appropriate level.
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
		 * A unique parent/kid combination. Set up BackendFunctionCallInfo.
		 * TODO: Measure if it makes sense to cache the name lookups in a HTAB
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
 
		ReleaseSysCache(tp);
	} 

	elog(INFO, "START: function %s.%s(%d) oid=%u parent=%u grandparent=%u", 
		bfci->f_schema, bfci->f_name, bfci->f_nargs,
		key.f_kid, key.f_parent, current_function_parent);
	current_depth++;

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

	/* We need to "pop" the current function oid and parent */
	current_function_oid = key.f_parent;
	current_function_parent = linitial_oid(call_stack);

	elog(INFO, "END: function %s.%s(%d) oid=%u parent=%u grandparent=%u", 
		bfci->f_schema, bfci->f_name, bfci->f_nargs,
		key.f_kid, key.f_parent, current_function_parent);
	current_depth--;

	call_stack = list_delete_first(call_stack);

	MemoryContextSwitchTo(oldctx);
}

static void start_table_stat(Relation rel)
{
}

