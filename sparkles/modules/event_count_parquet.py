# Counts the number of events from start to end time in a given window of fixed interval
import h5py
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, LongType
from datetime import datetime, date, timedelta
import sys
from operator import add
from pyspark.sql import *
import os
import json
from sparkles.modules.utils.helper import saveFeatures
from os.path import dirname


# Hash the keys into different interval periods
def keymod(x, start_time, interval):

    curr_t = x.created / 1000.0
    curr_t = curr_t - start_time
    keyindex = int(curr_t / interval)
    return (keyindex, 1)


# Transform the final time
def timetr(x, start_time, interval):

    dt = datetime.fromtimestamp(start_time + x[0] * interval).strftime('%Y-%m-%d %H:%M:%S.%f')  # x[0] is keyindex
    return (dt, x[1])  # x[1] is the total aggregated count


def saveResult(configpath, x, sqlContext, userdatadir, featureset_name, description, details, modulename, module_parameters, parent_datasets):

    schemaString = "timestamp count"

    fields_rdd = []
    for field_name in schemaString.split():
        if(field_name == 'count'):
            fields_rdd.append(StructField(field_name, IntegerType(), True))
        else:
            fields_rdd.append(StructField(field_name, StringType(), True))

    schema_rdd = StructType(fields_rdd)
    dfRdd = sqlContext.createDataFrame(x, schema_rdd)
    saveFeatures(configpath, dfRdd, userdatadir, featureset_name, description, details, modulename, json.dumps(module_parameters), json.dumps(parent_datasets))


def main(argv):
    conf = SparkConf()
    conf.setAppName("Parquet Count 60")
    conf.set("spark.jars", "file:/shared_data/spark_jars/hadoop-openstack-3.0.0-SNAPSHOT.jar")
    sc = SparkContext(conf=conf)

    # Swift Connection
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

    helperpath = str(argv[1])
    sc.addFile(helperpath + "/utils/helper.py")  # To import custom modules

    params = json.loads(str(argv[2]))
    inputs = json.loads(str(argv[3]))
    features = json.loads(str(argv[4]))

    userdatadir = str(features['userdatadir'])
    description = str(features['description'])
    details = str(features['details'])
    featureset_name = str(features['featureset_name'])
    modulename = str(features['modulename'])
    configpath = str(features['configpath'])

    tableindex = {"ORDERS": 3, "CANCELS": 4}
    tablename = str(params['tablename'])

    start_time = int(params['start_time']) / 1000.0
    s = int(params['start_time'])

    end_time = int(params['end_time']) / 1000.0
    e = int(params['end_time'])

    interval = float(params['interval'])

    index = tableindex[tablename]

    filepath = str(inputs[0])  # Provide the complete path
    filename = os.path.basename(os.path.abspath(filepath))
    tablepath = filepath + '/' + filename + '_' + str.lower(tablename) + '.parquet'

    sqlContext = SQLContext(sc)
    rdd = sqlContext.parquetFile(tablepath)

    # cc = rdd.count()
    # print(cc)  # Check how much total data is there

    # rdd = rdd.filter(start_time <= rdd.created / 1000.0 < end_time)  # Filter data according to the start and end times

    rdd.registerTempTable(tablename)
    rdd = sqlContext.sql("SELECT created FROM " + tablename + " WHERE created <" + str(e) + " AND created >=" + str(s))

    # dd = rdd.collect()
    # for kk in dd:
    #    print(kk)

    rdd1 = rdd.map(lambda x: keymod(x, start_time, interval)).reduceByKey(add)
    rdd1 = rdd1.sortByKey()
    rdd1 = rdd1.map(lambda x: timetr(x, start_time, interval))  # Human readable time

    parent_datasets = []
    parent_datasets.append(filename)  # Just append the names of the dataset used not the full path (Fetched from metadata)
    saveResult(configpath, rdd1, sqlContext, userdatadir, featureset_name, description, details, modulename, params, parent_datasets)

    d = rdd1.collect()

    for k in d:
        print(k)
        # print(v)

    sc.stop()


if __name__ == "__main__":
    main(sys.argv)
