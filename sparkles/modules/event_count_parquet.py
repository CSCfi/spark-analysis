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


def import_hdf5(x, filepath):

    filepath = '//shared_data//files//' + filepath
    with h5py.File(filepath) as f:
        data = f[str(x)].get('ORDERS')
        return list(data[:])


# Hash the keys into different interval periods
def keymod(x):

    curr_t = x.created / 1000.0
    curr_t = curr_t - start_time
    keyindex = int(curr_t / interval)
    return (keyindex, 1)


# Transform the final time
def timetr(x):

    dt = datetime.fromtimestamp(start_time + x[0] * interval).strftime('%Y-%m-%d %H:%M:%S.%f')
    return (dt, x[1])


def main(argv):
    conf = SparkConf()
    conf.setAppName("Parquet Count 60")
    conf.set("spark.executor.memory", "5g")
    sc = SparkContext(conf=conf)

    tableindex = {"ORDERS": 3, "CANCELS": 4}
    tablechoice = argv[1]

    global start_time  # Using public var as params are not allowed to pass in the mapper function
    start_time = int(argv[2]) / 1000.0
    s = int(argv[2])
    # start_time = 1349159961141 / 1000.0
    global end_time
    end_time = int(argv[3]) / 1000.0
    e = int(argv[3])
    # end_time = 1349160643981 / 1000.0
    global interval
    interval = float(argv[4])
    # interval = 60.0
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
    rdd = sqlContext.sql("SELECT created FROM orders WHERE created <" + str(e) + " AND created >=" + str(s))

    # dd = rdd.collect()
    # for kk in dd:
    #    print(kk)

    rdd1 = rdd.map(keymod).reduceByKey(add)
    rdd1 = rdd1.sortByKey()
    rdd1 = rdd1.map(timetr)  # Human readable time
    d = rdd1.collect()

    for k in d:
        print(k)
        # print(v)

    sc.stop()


if __name__ == "__main__":
    main(sys.argv)
