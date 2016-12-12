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
 [ ] Simple table access
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
	def __init__(self, r):
		self.object_oid = r[0]
		self.parent_oid = r[1]
		self.object_type = r[2]
		self.object_schema = r[3]
		self.object_name = r[4]
		self.num_calls = r[5]
		self.num_scans = r[6]
		self.total_time = r[7]
		self.self_time = r[8]

	def __str__(self):
		return "%s %s.%s oid=%d parent=%d calls=%d scans=%d total=%d self=%d" % \
			(self.object_type, self.object_schema, self.object_name,
			 self.object_oid, self.parent_oid, self.num_calls, self.num_scans,
			 self.total_time, self.self_time)

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
	stat_query = """
		SELECT object_oid,
               parent_oid,
               object_type,
               object_schema,
               object_name,
               num_calls,
               num_scans,
               total_time,
               self_time
         FROM  pg_stat_usage
		"""
	pgsu = PGStatUsage()
	for r in run_sql_query(con, stat_query):
		ur = PGStatUsageRecord(r)
		pgsu.set(ur.object_oid, ur.parent_oid, ur)
	return pgsu

class FunctionCallTester:

	def __init__(self):
		self.function_oids = {}
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

			# a small sleep is needed for the stats collector to catch up
			time.sleep(0.5)

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
		self.function_oids[function_name] = oid

	def _perform_assertion(self, results, func_name, func_parent_name, assert_function, expected_value):
		func_id = self.function_oids[func_name]
		if func_parent_name:
			parent_id = self.function_oids[func_parent_name]
		else:
			parent_id = 0
		usage_data = results.get(func_id, parent_id)

		failed = False
		status_msg = ""

		if not usage_data:
			failure, msg = True, "no usage data!"
		else:
			failure, msg = assert_function(usage_data, expected_value)

		print '%s %s -> %s %s' % (failure and "FAIL" or "PASS", func_parent_name, func_name, msg)

		if failure:
			print "Usage stats dump"
			print "----------------"
			for r in results.all():
				print r

	def assert_total_time(self, results, func_name, func_parent_name, duration_ms):
		"""Validate if total_time meets our expectations"""

		def _fn_assert_total_time(usage_data, expect_total_time_ms):
			tolerance_ms = 10
			total_time_ms = int(usage_data.total_time / 1000)
			if abs(total_time_ms - expect_total_time_ms) > tolerance_ms:
				return True, "assert_total_time() %d != %d" % \
							 (total_time_ms, expect_total_time_ms)
			return False, "assert_total_time() %d ~ %d" % \
				(total_time_ms, expect_total_time_ms)

		self._perform_assertion(results, func_name, func_parent_name,
								_fn_assert_total_time, duration_ms)

	def assert_self_time(self, results, func_name, func_parent_name, duration_ms):
		"""Validate if self_time meets our expectations
		"""
		def _fn_assert_self_time(usage_data, expect_self_time_ms):
			tolerance_ms = 10
			self_time_ms = int(usage_data.self_time / 1000)
			if abs(self_time_ms - expect_self_time_ms) > tolerance_ms:
				return True, "assert_self_time() %d != %d" % \
							 (self_time_ms, expect_self_time_ms)
			return False, "assert_self_time() %d ~ %d" % \
				(self_time_ms, expect_self_time_ms)

		self._perform_assertion(results, func_name, func_parent_name,
								_fn_assert_self_time, duration_ms)

	def assert_num_calls(self, results, func_name, func_parent_name, expect_num_calls):
		"""Validate if number of calls matches expected"""
		def _fn_assert_num_calls(usage_data, expect_num_calls):
			if usage_data.num_calls != expect_num_calls:
				return True, "assert_num_calls() %d != %d" % (usage_data.num_calls, expect_num_calls)
			return False, "assert_num_calls() %d == %d" % (usage_data.num_calls, expect_num_calls)

		self._perform_assertion(results, func_name, func_parent_name,
								_fn_assert_num_calls, expect_num_calls)

	def execute_functions(self, funcnames):
		"""Run the functions and collect usage data"""
		run_sql_query(self.con, "SELECT * FROM pg_stat_usage_reset()")
		for f in funcnames:
			run_sql_query(self.con, "SELECT * FROM %s()" % f, do_commit=False)
		return collect_stat_usage(self.con)

	def execute_function(self, funcname):
		"""Run a single function and collect usage data"""
		return self.execute_functions([funcname])

	def test_01_simple_function(self):
		self.create_simple_function("ff1", "PERFORM pg_sleep(0.010);")
		r = self.execute_function("ff1")

		self.assert_num_calls(r, "ff1", None, 1)
		self.assert_total_time(r, "ff1", None, 10)
		self.assert_self_time(r, "ff1", None, 10)

	def test_02_nested_functions(self):
		self.create_simple_function("ff2", "PERFORM ff1();")
		r = self.execute_function("ff2")

		self.assert_num_calls(r, "ff2", None, 1)
		self.assert_total_time(r, "ff2", None, 10)
		self.assert_self_time(r, "ff2", None, 1)

		self.assert_num_calls(r, "ff1", "ff2", 1)
		self.assert_total_time(r, "ff1", "ff2", 10)
		self.assert_self_time(r, "ff1", "ff2", 10)

	def test_03_nested_functions(self):
		r = self.execute_functions(["ff1", "ff2" ])
		self.assert_num_calls(r, "ff1", None, 1)
		self.assert_total_time(r, "ff1", None, 10)
		self.assert_self_time(r, "ff1", None, 10)

		self.assert_num_calls(r, "ff2", None, 1)
		self.assert_total_time(r, "ff2", None, 10)
		self.assert_self_time(r, "ff2", None, 1)

		self.assert_num_calls(r, "ff1", "ff2", 1)
		self.assert_total_time(r, "ff1", "ff2", 10)
		self.assert_self_time(r, "ff1", "ff2", 10)

	def test_04_call_count_is_cumulative(self):
		"""Validate that the counts are cumulative"""
		r = self.execute_functions(["ff2", "ff2" ])

		self.assert_num_calls(r, "ff2", None, 2)
		self.assert_total_time(r, "ff2", None, 20)
		self.assert_self_time(r, "ff2", None, 1)

		self.assert_num_calls(r, "ff1", "ff2", 2)
		self.assert_total_time(r, "ff1", "ff2", 20)
		self.assert_self_time(r, "ff1", "ff2", 20)

if __name__ == "__main__":
	fctest = FunctionCallTester()
	fctest.run_all_tests()

