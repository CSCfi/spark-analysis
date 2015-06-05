# Counts the number of events from start to end time in a given window of fixed interval
import h5py
from pyspark import SparkConf, SparkContext
from datetime import datetime, date, timedelta
import sys
from operator import add
from pyspark.sql import *

start_time = 0
index = 0
interval = 0
end_time = 0


# Hack for using HDF5 datasets in Spark, also fetches the data from dataset using the dates provided by user
def add_date_then_query(x):
    start = date.fromtimestamp(start_time)
    end = date.fromtimestamp(end_time)

    delta = timedelta(days=1)
    filepath = '//shared_data//files//' + x
    with h5py.File(filepath) as curr_file:
        res = []
        while start <= end:
            currdate = start.strftime("%Y_%m_%d")
            if currdate in curr_file:
                dategrp = curr_file[currdate]
                datedata = dategrp.get('ORDERS')
                res.append(list(datedata[:]))
            start += delta
        return sum(res, [])


# Hash the keys into different time interval periods and prices
def keymod(x):

    curr_t = x.created / 1000.0
    curr_t = curr_t - start_time
    keyindex = int(curr_t / interval)
    return (str(keyindex) + ' ' + str(x.price), x.quantity)


# Transform the final time
def timetr(x):

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

    tableindex = {"ORDERS": 3, "CANCELS": 4}
    tablechoice = argv[1]

    global start_time  # Using public var as params are not allowed(or I don't know of) to pass in the mapper function
    start_time = int(argv[2]) / 1000.0
    s = int(argv[2])

    global end_time
    end_time = int(argv[3]) / 1000.0
    e = int(argv[3])

    global interval
    interval = float(argv[4])
    global index
    index = tableindex[tablechoice]
    filepath = argv[5]

    filepath = "file:///shared_data//paths//" + filepath
    sqlContext = SQLContext(sc)
    rdd = sqlContext.parquetFile(filepath)

    # cc = rdd.count()
    # print(cc)  # Check how much total data is there

    # rdd = rdd.filter(start_time <= rdd.created / 1000.0 < end_time)  # Filter data according to the start and end times

    rdd.registerTempTable("orders")
    rdd = sqlContext.sql("SELECT created, side, price, quantity FROM orders WHERE created <" + str(e) + " AND created >=" + str(s))

    rdd1 = rdd.filter(rdd.side == 66)
    rdd2 = rdd.filter(rdd.side == 83)

    rdd1 = rdd1.map(keymod)  # Key hashing
    rdd2 = rdd2.map(keymod)

    # dd = rdd1.collect()
    # for k in dd:
    #    print(k)

    rdd1 = rdd1.reduceByKey(testred)  # Sum quantities
    rdd2 = rdd2.reduceByKey(testred)

    rdd1 = rdd1.map(keysplit)  # Change the key to time period
    rdd2 = rdd2.map(keysplit)

    rdd1 = rdd1.groupByKey().map(lambda x: (x[0], list(x[1])))  # Group the prices and summed quantities into one time period
    rdd2 = rdd2.groupByKey().map(lambda x: (x[0], list(x[1])))

    rdd1 = rdd1.map(timetr)  # Human readable time
    rdd2 = rdd2.map(timetr)

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
