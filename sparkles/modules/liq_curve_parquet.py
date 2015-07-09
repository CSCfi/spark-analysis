# Counts the number of events from start to end time in a given window of fixed interval
import h5py
from pyspark import SparkConf, SparkContext
from datetime import datetime, date, timedelta
import sys
from operator import add
from pyspark.sql import *
import os


# Hash the keys into different time interval periods and prices
def keymod(x, start_time, interval):

    curr_t = x.created / 1000.0
    curr_t = curr_t - start_time
    keyindex = int(curr_t / interval)
    return (str(keyindex) + ' ' + str(x.price), x.quantity)


# Transform the final time
def timetr(x, start_time, interval):

    dt = datetime.fromtimestamp(start_time + x[0] * interval).strftime('%Y-%m-%d %H:%M:%S.%f')
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
    # conf.setMaster("spark://nandan-spark-cluster-fe:6066")
    # conf.setMaster("local")
    conf.setAppName("Liq Cost Parquet")
    conf.set("spark.executor.memory", "5g")
    sc = SparkContext(conf=conf)

    params = str(argv[1]).split(',')
    inputs = str(argv[2]).split(',')

    tableindex = {"ORDERS": 3, "CANCELS": 4}
    tablename = str(params[0])

    start_time = int(params[1]) / 1000.0
    s = int(params[1])

    end_time = int(params[2]) / 1000.0
    e = int(params[2])

    interval = float(params[3])

    index = tableindex[tablename]

    filepath = str(inputs[0])  # Provide the complete path
    filename = os.path.basename(os.path.abspath(filepath))

    tablepath = filepath + '/' + filename + '_' + tablename + '.parquet'

    # filepath = "file:///shared_data//paths//" + filepath
    print(tablepath)
    sqlContext = SQLContext(sc)
    rdd = sqlContext.parquetFile(tablepath)

    # cc = rdd.count()
    # print(cc)  # Check how much total data is there

    # rdd = rdd.filter(start_time <= rdd.created / 1000.0 < end_time)  # Filter data according to the start and end times

    rdd.registerTempTable(tablename)
    rdd = sqlContext.sql("SELECT created, side, price, quantity FROM " + tablename + " WHERE created <" + str(e) + " AND created >=" + str(s))

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

    # rdd = rdd.sortByKey()
    # rdd = rdd.map(timetr)
    d = rdd.collect()

    for k in d:  # Print out the results
        print(k)
    #    print(v)

    sc.stop()


if __name__ == "__main__":
    main(sys.argv)
