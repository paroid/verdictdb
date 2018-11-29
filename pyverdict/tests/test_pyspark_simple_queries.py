'''
    Copyright 2018 University of Michigan
 
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
'''
from datetime import datetime, date, timezone
from pyspark.sql.session import SparkSession
import os
import pyverdict
import uuid

test_schema = 'pyverdict_spark_simple_query_test_schema' + str(uuid.uuid4())[:3]
test_table = 'test_table'
test_scramble = 'test_scramble'

spark = None
verdict = None


def setup_module(module):
    global spark, verdict
    spark = SparkSession.Builder().appName("test").enableHiveSupport().getOrCreate()

    # create table and populate data
    spark.sql("DROP TABLE IF EXISTS {}.{}".format(test_schema, test_table))
    spark.sql("DROP TABLE IF EXISTS {}.{}".format(test_schema, test_scramble))
    spark.sql('DROP SCHEMA IF EXISTS {}'.format(test_schema))
    spark.sql('CREATE SCHEMA IF NOT EXISTS ' + test_schema)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS {}.{} (
          intCol              INTEGER
        )""".format(test_schema, test_table))

    sql = 'insert into {}.{} values'.format(test_schema, test_table)
    for i in range(2000):
        if i == 0:
            sql = sql + ' '
        else:
            sql = sql + ', '
        sql = sql + '({:d})'.format(i)
    spark.sql(sql)

    # setup verdict
    verdict = pyverdict.spark(spark)
    verdict.sql('CREATE SCRAMBLE {}.{} FROM {}.{} BLOCKSIZE 200'
                .format(test_schema, test_scramble, test_schema, test_table))


def teardown_module(module):
    spark.sql('DROP SCHEMA IF EXISTS {} CASCADE'.format(test_schema))


class TestClass:

    def test_simple_bypass_count(self):
        sql = 'select count(*) from {}.{}'.format(test_schema, test_table)
        self.compare_bypass_query_result(sql)

    def test_simple_count(self):
        sql = 'select count(*) from {}.{}'.format(test_schema, test_table)
        self.compare_approximate_query_result(sql)

    def test_verdict_block_count(self):
        sql = """bypass select min(verdictdbblock), max(verdictdbblock)
            from {}.{}""".format(test_schema, test_scramble)
        result = verdict.sql_raw_result(sql)
        types = result.types()
        # rows = [[x.toString() for x in row] for row in result.rows()]
        rows = result.rows()
        print(types)
        print('block counts: ' + str(rows))
        assert 0 == rows[0][0]
        assert 9 == rows[0][1]

    def compare_query_result(self, verdict_sql, regular_sql):
        result = verdict.sql_raw_result(verdict_sql)
        types = result.types()
        rows = result.rows()
        # print(int_types)
        # print(types)
        print('verdict query: ' + str(verdict_sql))
        print('result: ' + str(rows))
        # print([type(x) for x in rows[0]])

        df = spark.sql(regular_sql)
        expected_rows = df.collect()
        print('presto query: ' + str(regular_sql))
        print('result: ' + str(expected_rows))

        # Now test
        assert len(expected_rows) == len(rows)
        assert len(expected_rows) == result.rowcount

        for i in range(len(expected_rows)):
            expected_row = expected_rows[i]
            actual_row = rows[i]
            for j in range(len(expected_row)):
                coltype = types[j]
                self.compare_value(expected_row[j], actual_row[j], coltype)

    def compare_approximate_query_result(self, sql):
        result = verdict.sql_raw_result(sql)
        types = result.types()
        rows = result.rows()
        print('approx answer: ' + str(types))
        print('approx results: ' + str(rows))
        df = spark.sql(sql)
        expected_rows = df.collect()

        # Now test
        assert len(expected_rows) == len(rows)
        assert len(expected_rows) == result.rowcount
        for i in range(len(expected_rows)):
            expected_row = expected_rows[i]
            actual_row = rows[i]
            for j in range(len(expected_row)):
                self.approximate_compare_value(expected_row[j], actual_row[j])

    def compare_bypass_query_result(self, sql):
        verdict_sql = 'bypass ' + sql
        self.compare_query_result(verdict_sql, sql)

    def compare_value(self, expected, actual, coltype):
        assert expected == actual

    def approximate_compare_value(self, expected, actual):
        assert expected is not None
        assert actual is not None
        assert expected * 1.2 >= actual
        assert expected * 0.8 <= actual
        