import h5py
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, LongType
from datetime import datetime, date, timedelta
import sys
from operator import add
from pyspark.sql import *
import os
import json
from sparkles.modules.utils.helper import saveFeatures  # If you need to save result as a feature set
from os.path import dirname
import argparse
import time
import calendar


# A General dummy function to be used in Map , it transforms the epoch time into human readable format
# def timetr(x):
#    dt = datetime.fromtimestamp(x.timefield).strftime('%Y-%m-%d %H:%M:%S.%f')
#    return (dt, x)

# This function is used only when you want to save your results as a feature set
def saveResult(configpath, x, sqlContext, userdatadir, featureset_name, description, details, modulename, module_parameters, parent_datasets):

    schemaString = "field1 field2"

    fields_rdd = []
    for field_name in schemaString.split():
        if(field_name == 'field1'):
            fields_rdd.append(StructField(field_name, IntegerType(), True))
        else:
            fields_rdd.append(StructField(field_name, StringType(), True))

    schema_rdd = StructType(fields_rdd)
    dfRdd = sqlContext.createDataFrame(x, schema_rdd)
    saveFeatures(configpath, dfRdd, userdatadir, featureset_name, description, details, modulename, json.dumps(module_parameters), json.dumps(parent_datasets))


# This is the function where you will write your module with logic and implementation
def your_module_implementation(sc, params=None, inputs=None, features=None):

    tablename = str(params['tablename'])  # Mandatory parameter

    # The time would be provided by user as a string in this format "2011-12-29_14:43:47:141", we need to convert into epoch times for working

    # start_time_str = str(params['start_time'])
    # start_time = int(str(calendar.timegm(time.strptime( start_time_str[:-4],'%Y-%m-%d_%H:%M:%S'))) + start_time_str[-3:])  # convert to epoch

    # end_time_str = str(params['end_time'])
    # end_time = int(str(calendar.timegm(time.strptime( end_time_str[:-4],'%Y-%m-%d_%H:%M:%S'))) + end_time_str[-3:])  # convert to epoch

    # interval = float(params['interval'])

    filepath = str(inputs[0])  # Provide the complete path
    filename = os.path.basename(os.path.abspath(filepath))  # Don't change
    tablepath = filepath + '/' + filename + '_' + str.lower(tablename) + '.parquet'  # Don't change

    sqlContext = SQLContext(sc)  # Create SQLContext var from SparkContext, Useful only if you are using dataframes i.e. parquet files
    rdd = sqlContext.read.parquet(tablepath)
    rdd.registerTempTable("tablename")  # Register the table in parquet file for our usage with SQL queries
    rdd = sqlContext.sql("SELECT fields from tablename WHERE field_name == criteria")  # A dataframe is returned which can be used as an RDD

    # rdd1 = rdd.filter(rdd.somefieldname == somecriteria)  # Example of filtering
    # rdd1 = rdd1.map(somemapfunction)  # Applying transformation to the RDD by passing custom functions to map
    # rdd1 = rdd1.reduceByKey(somereducefunction)  # For performing sum or average of the results
    # rdd1 = rdd1.groupByKey()  # Useful for grouping the results by key
    # rdd1 = rdd1.map(timetr)  # If you need human readable time

    # d = rdd.collect()  #Collect the results before displaying
    # for k in d:  # Print out the results
    #    print(k)

    # If you need to save the results as feature set use this parameters as well
    # userdatadir = str(features['userdatadir'])
    # description = str(features['description'])
    # details = str(features['details'])
    # featureset_name = str(features['featureset_name'])
    # modulename = str(features['modulename'])

    # configpath = str(features['configpath'])  # Automatically supplied, don't change anything

    # For saving the results to a parquet file
    # Convert the RDD to a dataframe first by defining the schema

    # parent_datasets = []
    # parent_datasets.append(filename)  # Just append the names of the dataset used not the full path (Fetched from metadata)
    # saveResult(configpath, rdd1, sqlContext, userdatadir, featureset_name, description, details, modulename, params, parent_datasets)

    sc.stop()


def main():

    # Configure Spark
    conf = SparkConf()
    conf.setAppName("Application name")  # Specify the application name
    conf.set("spark.jars", "file:/shared_data/spark_jars/hadoop-openstack-3.0.0-SNAPSHOT.jar")  # Don't modify
    sc = SparkContext(conf=conf)  # Spark Context variable that will be used for all operations running on the cluster

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

    helperpath = str(args.helperpath)  # This is passed by default
    sc.addFile(helperpath + "/utils/helper.py")  # To import custom modules

    # Create a dict and pass it in your_module_implementation
    params = json.loads(args.params)
    inputs = json.loads(args.inputs)
    # features = json.loads(args.features)  # Only used when you want to create a feature set

    your_module_implementation(sc, params=params, inputs=inputs, features=features)


if __name__ == "__main__":
    main()
