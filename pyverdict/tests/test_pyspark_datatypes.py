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

test_schema = 'pyverdict_spark_datatype_test_schema' + str(uuid.uuid4())[:3]
test_table = 'test_table'


def test_data_types():
    (spark, verdict) = setup_sandbox()

    result = verdict.sql_raw_result(
        'select * from {}.{} order by tinyintCol'.format(
            test_schema, test_table))
    types = result.types()
    rows = result.rows()
    print(types)
    print(rows)
    print([[type(x) for x in row] for row in rows])

    df = spark.sql('select * from {}.{} order by tinyintCol'.format(
                  test_schema, test_table))
    expected_rows = df.collect()
    print(expected_rows)

    # Now test
    assert len(expected_rows) == len(rows)
    assert len(expected_rows) == result.rowcount

    for i in range(len(expected_rows)):
        expected_row = expected_rows[i]
        actual_row = rows[i]
        for j in range(len(expected_row)):
            compare_value(expected_row[j], actual_row[j], types[j])
    tear_down(spark)

def compare_value(expected, actual, coltype):
    assert expected == actual

def setup_sandbox():
    '''
    We test the data types defined here:
    https://docs.tibco.com/pub/sfire-analyst/7.7.1/doc/html/en-US/TIB_sfire-analyst_UsersGuide/connectors/apache-spark/apache_spark_data_types.htm

    That is:
    1. TINYINT
    2. SMALLINT
    3. INT
    4. BIGINT
    5. FLOAT
    6. DOUBLE
    7. BOOLEAN
    8. STRING
    9. TIMESTAMP
    '''
    spark = SparkSession.Builder().appName("test").enableHiveSupport().getOrCreate()

    # create table and populate data
    spark.sql("DROP TABLE IF EXISTS {}.{}".format(test_schema, test_table))
    spark.sql('DROP SCHEMA IF EXISTS {}'.format(test_schema))
    spark.sql('CREATE SCHEMA IF NOT EXISTS ' + test_schema)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            tinyintCol          TINYINT,
            boolCol             BOOLEAN,
            smallintCol         SMALLINT,
            integerCol          INT,
            bigintCol           BIGINT,
            floatCol            FLOAT,
            doubleCol           DOUBLE,
            timestampCol        TIMESTAMP,
            timestampCol2       TIMESTAMP,
            stringCol           STRING
        )""".format(test_schema, test_table)
        )

    spark.sql("""
        INSERT INTO {}.{} VALUES (
            cast(1 as tinyint), 
            true,
            cast(2 as smallint),
            cast(3 as int),
            cast(4 as bigint),
            cast(5.0 as float),
            cast(6.0 as double),
            timestamp '2018-12-31 00:00:01',
            timestamp '2018-12-31 00:00:01.001',
            'hello world'
        )""".format(test_schema, test_table)
        )

    spark.sql("""
        INSERT INTO {}.{} VALUES (
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL
        )""".format(test_schema, test_table)
        )

    verdict = pyverdict.spark(spark)
    return (spark, verdict)

def tear_down(spark):
    spark.sql('DROP SCHEMA IF EXISTS {} CASCADE'.format(test_schema))


