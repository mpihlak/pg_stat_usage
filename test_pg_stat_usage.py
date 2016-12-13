#!/usr/bin/env python

import psycopg2
import time

"""
Generate testing functions.
Run the testing functions and validate resutls.

XXX: Totally ignoring any namespaces at this point! For now just assume that
there are no naming clashes.

Test cases:
 [x] Simple function call
 [x] Simple table access
 [x] Nested function calls
 [ ] Nested function calls with table accesses
 [ ] Nested function call with an open cursor
 [ ] Recursive function call
 [ ] Recursive function call with table access

Design goals
 * Make it easy to define additional test cases
 * Validation is automatic
"""

debug_mode = False

TIME_DIFF_TOLERANCE = 10

def debug(msg):
	if debug_mode:
		print msg

def _run_sql(con, sql, do_fetch=False, do_commit=True):
	debug("running sql: %s" % sql.lstrip().split('\n')[0])
	cur = con.cursor()
	cur.execute(sql)
	if do_fetch:
		r = cur.fetchall()
	else:
		r = None
	if do_commit:
		con.commit()
	return r

def run_sql_cmd(con, sql, do_commit=True):
	return _run_sql(con, sql, do_fetch=False, do_commit=do_commit)

def run_sql_query(con, sql, do_commit=True):
	return _run_sql(con, sql, do_fetch=True, do_commit=do_commit)

def create_connection(dbname=""):
	"Create a new database connection and prepare environment"
	con = psycopg2.connect("")
	run_sql_cmd(con, 'SET track_functions="all"')
	run_sql_cmd(con, "LOAD 'pg_stat_usage'")
	return con

class PGStatUsageRecord:
	def __init__(self, columns, values):
		self.items = dict(zip(columns, values))
		self.object_oid = self.items['object_oid']
		self.parent_oid = self.items['parent_oid']
		# times to ms
		self.items['total_time'] /= 1000
		self.items['self_time'] /= 1000

	def get(self, item_name):
		return self.items.get(item_name)

	def __str__(self):
		keys = self.items.keys()
		keys.sort()
		vstr = " ".join([ "%s=%d" % (k, self.items[k]) for k in keys ])
		return vstr

class PGStatUsage:
	def __init__(self):
		self.usage_dict = {}
	def set(self, obj_oid, parent_oid, ur):
		self.usage_dict[(ur.object_oid, ur.parent_oid)] = ur
	def get(self, obj_oid, parent_oid):
		return self.usage_dict[(obj_oid, parent_oid)]
	def all(self):
		return self.usage_dict.values()

def collect_stat_usage(con):
	stat_query = "SELECT * FROM pg_stat_usage"

	pgsu = PGStatUsage()
	cur = con.cursor()
	cur.execute(stat_query)
	columns = [ c.name for c in cur.description ]
	for values in cur.fetchall():
		ur = PGStatUsageRecord(columns, values)
		pgsu.set(ur.object_oid, ur.parent_oid, ur)
	con.commit()
	return pgsu

class FunctionCallTester:

	def __init__(self):
		self.object_oids = {}
		self.con = create_connection()

	def run_all_tests(self):
		test_methods = []
		for method in dir(self):
			if callable(getattr(self, method)) and method.startswith("test_"):
				test_methods.append(method)
		test_methods.sort()

		for method in test_methods:
			print "%s Running test: %s" % ('=' * 10, method)
			eval("self.%s()" % method)

	def create_simple_function(self, function_name, function_body):
		create_statement = """
			CREATE OR REPLACE FUNCTION %s() RETURNS void AS
			$$
			BEGIN
			%s
			END
			$$ LANGUAGE plpgsql
			""" % (function_name, function_body)
		run_sql_cmd(self.con, create_statement)

		lookup_oid = """
			SELECT oid FROM pg_proc WHERE proname='%s'
			""" % function_name
		oid = run_sql_query(self.con, lookup_oid)[0][0]
		self.object_oids[function_name] = oid

	def create_table(self, table_name, table_body):
		create_statement = """
			DROP TABLE IF EXISTS %s;
			CREATE TABLE %s (
			%s
			)
			""" % (table_name, table_name, table_body);
		run_sql_cmd(self.con, create_statement)

		lookup_oid = """
			SELECT oid FROM pg_class WHERE relname='%s'
			""" % table_name
		oid = run_sql_query(self.con, lookup_oid)[0][0]
		self.object_oids[table_name] = oid

	def assert_value(self, results, obj_name, obj_parent_name, item_name, expected_value, tolerance=0):
		func_id = self.object_oids[obj_name]
		if obj_parent_name:
			parent_id = self.object_oids[obj_parent_name]
		else:
			parent_id = 0

		usage_data = results.get(func_id, parent_id)
		usage_value = usage_data.get(item_name)

		if abs(expected_value - usage_value) > tolerance:
			status = "FAIL"
			op = "!"

			debug("Usage stats dump")
			debug("----------------")
			for r in results.all():
				debug(r)
		else:
			status = "PASS"
			op = "="

		print "%s assert_value(%s -> %s, %s, %d) %s= %d" % \
			(status, obj_parent_name, obj_name, item_name, 
			 expected_value, op, usage_value)

	def execute_functions(self, funcnames):
		"""Run the functions and collect usage data"""
		run_sql_query(self.con, "SELECT * FROM pg_stat_usage_reset()")
		for f in funcnames:
			run_sql_query(self.con, "SELECT * FROM %s()" % f, do_commit=False)
		return collect_stat_usage(self.con)

	def execute_sql_statements(self, statements):
		"""Run the SQL statements and collect usage data"""
		run_sql_query(self.con, "SELECT * FROM pg_stat_usage_reset()")
		for stmt in statements:
			run_sql_cmd(self.con, stmt, do_commit=False)
		return collect_stat_usage(self.con)

	def execute_function(self, funcname):
		"""Run a single function and collect usage data"""
		return self.execute_functions([funcname])

	def execute_single_statement(self, stmt):
		"""Run a single SQL statement and collect usage data"""
		return self.execute_sql_statements([stmt])

	def test_01_simple_function(self):
		self.create_simple_function("ff1", "PERFORM pg_sleep(0.010);")
		r = self.execute_function("ff1")

		self.assert_value(r, "ff1", None, "num_calls", 1)
		self.assert_value(r, "ff1", None, "total_time", 10, TIME_DIFF_TOLERANCE)
		self.assert_value(r, "ff1", None, "self_time", 10, TIME_DIFF_TOLERANCE)

	def test_02_nested_functions(self):
		self.create_simple_function("ff2", "PERFORM ff1();")
		r = self.execute_function("ff2")

		self.assert_value(r, "ff2", None, "num_calls", 1)
		self.assert_value(r, "ff2", None, "total_time", 10, TIME_DIFF_TOLERANCE)
		self.assert_value(r, "ff2", None, "self_time", 1, TIME_DIFF_TOLERANCE)

		self.assert_value(r, "ff1", "ff2", "num_calls", 1)
		self.assert_value(r, "ff1", "ff2", "total_time", 10, TIME_DIFF_TOLERANCE)
		self.assert_value(r, "ff1", "ff2", "self_time", 10, TIME_DIFF_TOLERANCE)

	def test_03_nested_functions(self):
		r = self.execute_functions(["ff1", "ff2" ])
		self.assert_value(r, "ff1", None, "num_calls", 1)
		self.assert_value(r, "ff1", None, "total_time", 10)
		self.assert_value(r, "ff1", None, "self_time", 10)

		self.assert_value(r, "ff2", None, "num_calls", 1)
		self.assert_value(r, "ff2", None, "total_time", 10, TIME_DIFF_TOLERANCE)
		self.assert_value(r, "ff2", None, "self_time", 1, TIME_DIFF_TOLERANCE)

		self.assert_value(r, "ff1", "ff2", "num_calls", 1)
		self.assert_value(r, "ff1", "ff2", "total_time", 10, TIME_DIFF_TOLERANCE)
		self.assert_value(r, "ff1", "ff2", "self_time", 10, TIME_DIFF_TOLERANCE)

	def test_04_call_count_is_cumulative(self):
		"""Validate that the counts are cumulative"""
		r = self.execute_functions(["ff2", "ff2" ])

		self.assert_value(r, "ff2", None, "num_calls", 2)
		self.assert_value(r, "ff2", None, "total_time", 20, TIME_DIFF_TOLERANCE)
		self.assert_value(r, "ff2", None, "self_time", 1, TIME_DIFF_TOLERANCE)

		self.assert_value(r, "ff1", "ff2", "num_calls", 2)
		self.assert_value(r, "ff1", "ff2", "total_time", 20, TIME_DIFF_TOLERANCE)
		self.assert_value(r, "ff1", "ff2", "self_time", 20, TIME_DIFF_TOLERANCE)

	def test_05_create_table(self):
		self.create_table("tt1", "i integer primary key, t text");
		r = self.execute_single_statement("INSERT INTO tt1 SELECT i, 't' FROM generate_series(1,100) as i");
		self.assert_value(r, "tt1", None, "num_scans", 0)
		self.assert_value(r, "tt1", None, "n_tup_ins", 100)

	def test_06_count_table(self):
		r = self.execute_single_statement("SELECT COUNT(*) FROM tt1")
		self.assert_value(r, "tt1", None, "num_scans", 1)
		self.assert_value(r, "tt1", None, "n_tup_ret", 100)

	def test_07_count_table_from_function(self):
		self.create_simple_function("ff4", "PERFORM COUNT(*) FROM tt1;")
		r = self.execute_function("ff4");
		self.assert_value(r, "tt1", "ff4", "num_scans", 1)

	def test_08_count_table_mixed(self):
		r = self.execute_sql_statements(["SELECT ff4()", "SELECT COUNT(*) FROM tt1"]);
		self.assert_value(r, "tt1", "ff4", "num_scans", 1)
		self.assert_value(r, "tt1", None, "num_scans", 1)

	def test_09_update_table_direct(self):
		r = self.execute_single_statement("UPDATE tt1 SET t='updated'")
		self.assert_value(r, "tt1", None, "num_scans", 1)
		self.assert_value(r, "tt1", None, "n_tup_upd", 100)

	def test_10_delete_table_direct(self):
		r = self.execute_single_statement("DELETE FROM tt1")
		self.assert_value(r, "tt1", None, "num_scans", 1)
		self.assert_value(r, "tt1", None, "n_tup_del", 100)

if __name__ == "__main__":
	fctest = FunctionCallTester()
	fctest.run_all_tests()

