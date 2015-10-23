# Counts the number of events from start to end time in a given window of fixed interval
import h5py
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, LongType, ArrayType, TimestampType
from datetime import datetime, date, timedelta
import sys
from operator import add
from pyspark.sql import *
import os
import json
from sparkles.modules.utils.helper import saveFeatures
import argparse
import time
from math import ceil
from itertools import takewhile
import calendar


# Transform all the destroy values which are zero to the end of the day's timestamp
def transform_zero_destroys(x):

    tc = x.created
    td = x.destroyed

    if(td == 0):
        tc_dt = datetime.fromtimestamp(tc / 1000)
        td_dt = datetime(tc_dt.year, tc_dt.month, tc_dt.day, 23, 59, 59)
        td = calendar.timegm(datetime.timetuple(td_dt)) * 1000
        td = td + 999  # Add the milliseconds

    return (tc, td, x.side, x.price, x.quantity)


# This logic uses reverse generation. It generates input timestamps for each of the records fetched from dataset
# It shows usage of generators as transformation function for Spark which is quite useful for advance querying
def generate_timestamps(start_time, end_time, interval):
    def _generate_timestamps(created, destroyed, side, price, qty):  # Alternative for accessing data using logical indexing
        start_time_row = int(ceil((created - start_time) / interval) * interval + start_time)
        filtered_timestamps = takewhile(lambda x: created <= x < destroyed, xrange(start_time_row, end_time, interval))  # Generation
        for ts in filtered_timestamps:
            yield (ts, price), qty  # Send the key as timestamp,price and value as qty (in order to sum qty later easily)

    return _generate_timestamps


# Sum the quantities for given price at a given time
def sum_qty_for_price(x):

    return (x[0][0], [x[0][1], sum(x[1])])  # Back to logical indexing! x[0][1] contains the price and x[1] the quantities


# Sort the pairs
def sorter(x):

    x.sort()
    return x


# Helper function to convert rdd to dataframe which is a more efficient for SQL operations like join
def rdd_to_dataframe(sqlContext, rdd, curve):

    schemaString = "timestamp " + curve

    fields_rdd = []
    for field_name in schemaString.split():
        if(field_name == curve):
            fields_rdd.append(StructField(field_name, ArrayType(ArrayType(IntegerType(), True), True), True))
        else:
            fields_rdd.append(StructField(field_name, LongType(), True))

    schema_rdd = StructType(fields_rdd)
    df = sqlContext.createDataFrame(rdd, schema_rdd)
    return df


# Save the data as parquet
def saveResult(configpath, dfRdd, sqlContext, userdatadir, featureset_name, description, details, modulename, module_parameters, parent_datasets):

    saveFeatures(configpath, dfRdd, userdatadir, featureset_name, description, details, modulename, json.dumps(module_parameters), json.dumps(parent_datasets))


def main():
    conf = SparkConf()
    conf.setAppName("Liq Cost Parquet")
    conf.set("spark.jars", "file:/shared_data/spark_jars/hadoop-openstack-3.0.0-SNAPSHOT.jar")
    sc = SparkContext(conf=conf)

    parser = argparse.ArgumentParser()
    parser.add_argument("backend", type=str)
    parser.add_argument("helperpath", type=str)
    parser.add_argument("params", type=str)
    parser.add_argument("inputs", type=str)
    parser.add_argument("features", type=str, nargs='?')

    args = parser.parse_args()

    # Swift Connection
    if(args.backend == 'swift'):
        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set("fs.swift.impl", "org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem")
        hadoopConf.set("fs.swift.service.SparkTest.auth.url", os.environ['OS_AUTH_URL'] + "/tokens")
        hadoopConf.set("fs.swift.service.SparkTest.http.port", "8443")
        hadoopConf.set("fs.swift.service.SparkTest.auth.endpoint.prefix", "/")
        hadoopConf.set("fs.swift.service.SparkTest.region", os.environ['OS_REGION_NAME'])
        hadoopConf.set("fs.swift.service.SparkTest.public", "false")
        hadoopConf.set("fs.swift.service.SparkTest.tenant", os.environ['OS_TENANT_ID'])
        hadoopConf.set("fs.swift.service.SparkTest.username", os.environ['OS_USERNAME'])
        hadoopConf.set("fs.swift.service.SparkTest.password", os.environ['OS_PASSWORD'])

    helperpath = args.helperpath
    sc.addFile(helperpath + "/utils/helper.py")  # To import custom modules

    params = json.loads(args.params)
    inputs = json.loads(args.inputs)
    features = json.loads(args.features)

    userdatadir = str(features['userdatadir'])
    description = str(features['description'])
    details = str(features['details'])
    featureset_name = str(features['featureset_name'])
    modulename = str(features['modulename'])
    configpath = str(features['configpath'])

    tablename = str(params['tablename'])

    start_time_str = str(params['start_time'])
    start_time = int(str(calendar.timegm(time.strptime(start_time_str[:-4], '%Y-%m-%d_%H:%M:%S'))) + start_time_str[-3:])  # convert to epoch

    end_time_str = str(params['end_time'])
    end_time = int(str(calendar.timegm(time.strptime(end_time_str[:-4], '%Y-%m-%d_%H:%M:%S'))) + end_time_str[-3:])  # convert to epoch

    interval = int(params['interval'])

    filepath = str(inputs[0])  # Provide the complete path
    filename = os.path.basename(os.path.abspath(filepath))
    tablepath = filepath + '/' + filename + '_' + str.lower(tablename) + '.parquet'

    sqlContext = SQLContext(sc)
    rdd = sqlContext.parquetFile(tablepath)

    rdd.registerTempTable(tablename)
    rdd = sqlContext.sql("SELECT created, destroyed, side, price, quantity FROM " + tablename + " WHERE created <=" + str(end_time) + " AND destroyed >" + str(start_time))

    rdd = rdd.map(lambda x: transform_zero_destroys(x))

    rdd1 = rdd.filter(lambda x: x[2] == 66)  # Filter records for Buy, logical index 2 represents Side here
    rdd2 = rdd.filter(lambda x: x[2] == 83)  # Filter records for Sell

    rdd1 = rdd1.flatMap(lambda x: generate_timestamps(start_time, end_time, interval)(*x)).groupByKey()
    rdd2 = rdd2.flatMap(lambda x: generate_timestamps(start_time, end_time, interval)(*x)).groupByKey()

    rdd1 = rdd1.map(lambda x: sum_qty_for_price(x)).groupByKey()
    rdd2 = rdd2.map(lambda x: sum_qty_for_price(x)).groupByKey()

    rdd1 = rdd1.mapValues(list)
    rdd1 = rdd1.mapValues(lambda x: sorter(x))

    rdd2 = rdd2.mapValues(list)
    rdd2 = rdd2.mapValues(lambda x: sorter(x))

    df_buy = rdd_to_dataframe(sqlContext, rdd1, "curve_buy")
    df_sell = rdd_to_dataframe(sqlContext, rdd2, "curve_sell")

    df_total = df_buy.join(df_sell, df_buy.timestamp == df_sell.timestamp)
    df_total = df_total.select(df_buy.timestamp, df_total.curve_buy, df_total.curve_sell)  # Just one timestamp field should be there not two!
    df_total = df_total.sort(df_total.timestamp)

    parent_datasets = []
    parent_datasets.append(filename)  # Just append the names of the dataset used not the full path (Fetched from metadata)
    saveResult(configpath, df_total, sqlContext, userdatadir, featureset_name, description, details, modulename, params, parent_datasets)  # Notice we pass here the dataframe not rdd because we have already created it

    for k in df_total.collect():  # Print out the results
        print(k)

    sc.stop()


if __name__ == "__main__":
    main()
