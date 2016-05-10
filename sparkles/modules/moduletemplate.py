import h5py
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, LongType
from datetime import datetime, date, timedelta
import sys
from operator import add
from pyspark.sql import SQLContext
import os
import json
from sparkles.modules.utils.helper import saveFeatures  # If you need to save result as a feature set
from os.path import dirname
import argparse
import time
import calendar


# This is the function where you will write your module with logic and implementation
def module_implementation(sc, sqlContext, params=None, inputs=None, features=None):

    filepath = str(inputs[0])  # If you are working with one dataset then just read the first element of the array
    dataframe = sqlContext.read.parquet(filepath)  # Now you get your dataset as a dataframe here

    dataframe.registerTempTable("tablename")  # Register the table in parquet file for our usage with SQL queries
    df = sqlContext.sql("SELECT fields from tablename WHERE field_name == criteria")  # A dataframe is returned which can be used as an RDD

    # Once you have df , you can perform transformations and other operations that you want and write down the code here
    # rdd = df.map(lambda x: x) just an example
    # More code here.....

    # Once you have written your code, provide the schema here to generate the resulting dataframe
    schemaString = "field1 field2"

    fields_df = []
    for field_name in schemaString.split():
        if(field_name == 'field1'):
            fields_df.append(StructField(field_name, IntegerType(), True))
        else:
            fields_df.append(StructField(field_name, StringType(), True))

    schema_df = StructType(fields_df)
    feature_dataframe = sqlContext.createDataFrame(rdd, schema_df)  # Dataframe for featureset created here

    # Don't change the lines below
    saveFeatures(feature_dataframe, features, params, inputs)  # Just pass the feature_dataframe which you generated for your results to this function. (features, params, inputs remain as it is)
    sc.stop()


# This is the main function which you do not have to modify!
def main():

    # Configure Spark
    conf = SparkConf()
    conf.setAppName("Application name")  # Specify the application name
    conf.set("spark.jars", "file:/shared_data/spark_jars/hadoop-openstack-3.0.0-SNAPSHOT.jar")  # Don't modify
    sc = SparkContext(conf=conf)  # Spark Context variable that will be used for all operations running on the cluster

    parser = argparse.ArgumentParser()
    parser.add_argument("backend", type=str)
    parser.add_argument("helperpath", type=str)
    parser.add_argument("shuffle_partitions", type=str)
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
    shuffle_partitions = args.shuffle_partitions

    # Create a dict and pass it in your_module_implementation
    params = json.loads(args.params)
    inputs = json.loads(args.inputs)
    features = json.loads(args.features)  # Only used when you want to create a feature set

    sqlContext = SQLContext(sc)  # Create SQLContext var from SparkContext, To work with our default format of datasets i.e. Parquet
    sqlContext.setConf("spark.sql.shuffle.partitions", shuffle_partitions)  # Don't change, required for controlling parallelism

    # Pass the sc (Spark Context) and sqlContext along with the different paramters and inputs.
    module_implementation(sc, sqlContext, params=params, inputs=inputs, features=features)

if __name__ == "__main__":
    main()
