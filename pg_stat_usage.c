#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/rel.h"

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

void _PG_fini(void)
{
	report_stat_hook = prev_report_stat_hook;
	start_function_stat_hook = prev_start_function_stat_hook;
	end_function_stat_hook = prev_end_function_stat_hook;
	start_table_stat_hook = prev_start_table_stat_hook;
}

static void report_stat(void)
{
	elog(INFO, "Dumping stats.");
}

static void start_function_stat(FunctionCallInfoData *fcinfo, PgStat_FunctionCallUsage *fcu)
{
	elog(INFO, "Starting function stats: oid=%u", (unsigned)fcinfo->flinfo->fn_oid);
}

static void end_function_stat(PgStat_FunctionCallUsage *fcu, bool finalize)
{
	elog(INFO, "Ending function stats");
}

static void start_table_stat(Relation rel)
{
	Oid rel_id = rel->rd_id;

	elog(INFO, "Starting table stats: oid=%u", (unsigned)rel_id);
}

