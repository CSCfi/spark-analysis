# Counts the number of events from start to end time in a given window of fixed interval
import h5py
from pyspark import SparkConf, SparkContext
from datetime import datetime, date, timedelta
import sys
from operator import add
from pyspark.sql import *
import os
import json
from sparkles.modules.utils.helper import saveFeatures


# Hash the keys into different time interval periods and prices
def keymod(x, start_time, interval):

    curr_t = x.created
    curr_t = curr_t - start_time
    keyindex = int(curr_t / interval)
    return (str(keyindex) + ' ' + str(x.price), x.quantity)


# Transform the final time
def timetr(x, start_time, interval):

    t = (start_time + x[0] * interval) / 1000.0
    dt = datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S.%f')  # x[0] is keyindex
    return (dt, x[1])


def testred(x, y):

    return x + y


# Change the key of the RDD from time,price to only time
def keysplit(x):

    res = x[0].split()
    return (int(res[0]), (int(res[1]), x[1]))


def flatten_lists(x):

    return (x[0], (sum(x[1][0], []), sum(x[1][1], [])))


def main(argv):
    conf = SparkConf()
    conf.setAppName("Liq Cost Parquet")
    conf.set("spark.jars", "file:/shared_data/spark_jars/hadoop-openstack-3.0.0-SNAPSHOT.jar")
    sc = SparkContext(conf=conf)

    # Swift Connection
    if(str(argv[1]) == 'swift'):
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

    helperpath = str(argv[2])
    sc.addFile(helperpath + "/utils/helper.py")  # To import custom modules

    params = json.loads(str(argv[3]))
    inputs = json.loads(str(argv[4]))

    tablename = str(params['tablename'])

    start_time = int(params['start_time'])

    end_time = int(params['end_time'])

    interval = float(params['interval'])

    filepath = str(inputs[0])  # Provide the complete path
    filename = os.path.basename(os.path.abspath(filepath))
    tablepath = filepath + '/' + filename + '_' + str.lower(tablename) + '.parquet'

    sqlContext = SQLContext(sc)
    rdd = sqlContext.parquetFile(tablepath)

    # rdd = rdd.filter(start_time <= rdd.created / 1000.0 < end_time)  # Filter data according to the start and end times

    rdd.registerTempTable(tablename)
    rdd = sqlContext.sql("SELECT created, side, price, quantity FROM " + tablename + " WHERE created <" + str(end_time) + " AND created >=" + str(start_time))

    rdd1 = rdd.filter(rdd.side == 66)
    rdd2 = rdd.filter(rdd.side == 83)

    rdd1 = rdd1.map(lambda x: keymod(x, start_time, interval))  # Key hashing
    rdd2 = rdd2.map(lambda x: keymod(x, start_time, interval))

    # dd = rdd1.collect()
    # for k in dd:
    #    print(k)

    rdd1 = rdd1.reduceByKey(testred)  # Sum quantities
    rdd2 = rdd2.reduceByKey(testred)

    rdd1 = rdd1.map(keysplit)  # Change the key to time period
    rdd2 = rdd2.map(keysplit)

    rdd1 = rdd1.groupByKey().map(lambda x: (x[0], list(x[1])))  # Group the prices and summed quantities into one time period
    rdd2 = rdd2.groupByKey().map(lambda x: (x[0], list(x[1])))

    rdd1 = rdd1.map(lambda x: timetr(x, start_time, interval))  # Human readable time
    rdd2 = rdd2.map(lambda x: timetr(x, start_time, interval))

    rdd = rdd1.cogroup(rdd2)  # Group the RDDs of Buy and Sell together on the basis of time period
    rdd = rdd.map(lambda x: (x[0], (list(x[1][0]), list(x[1][1]))))  # Iterable object to list
    rdd = rdd.map(flatten_lists)

    rdd = rdd.sortByKey()
    # rdd = rdd.map(timetr)
    d = rdd.collect()

    for k in d:  # Print out the results
        print(k)
    #    print(v)

    sc.stop()


if __name__ == "__main__":
    main(sys.argv)
