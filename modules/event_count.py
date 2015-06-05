# Counts the number of events from start to end time in a given window of fixed interval
import h5py
from pyspark import SparkConf, SparkContext
from datetime import datetime, date, timedelta
import sys
from operator import add


start_time = 0
index = 0
interval = 0
end_time = 0


def add_date(filename, start_time, end_time):
    start = date.fromtimestamp(start_time)
    end = date.fromtimestamp(end_time)

    delta = timedelta(days=1)
    filepath = '//shared_data//files//' + filename
    with h5py.File(filepath) as curr_file:
        with open('//shared_data//paths//keys1.txt', 'w') as keysfile:
            while start <= end:
                currdate = start.strftime("%Y_%m_%d")
                if currdate in curr_file:
                    print>>keysfile, currdate
                start += delta


def import_hdf5(x, filepath):

    filepath = '//shared_data//files//' + filepath
    with h5py.File(filepath) as f:
        data = f[str(x)].get('ORDERS')
        return list(data[:])


# Hash the keys into different interval periods
def keymod(x):

    curr_t = x[index] / 1000.0
    curr_t = curr_t - start_time
    keyindex = int(curr_t / interval)
    return (keyindex, 1)


# Transform the final time
def timetr(x):

    dt = datetime.fromtimestamp(start_time + x[0] * interval).strftime('%Y-%m-%d %H:%M:%S.%f')
    return (dt, x[1])


def module_description():

    descr = "Counts the events based on the given table and time period"
    return descr


def main(argv):
    conf = SparkConf()
    # conf.setMaster("spark://nandan-spark-cluster-fe:6066")
    # conf.setMaster("local")
    conf.setAppName("HDF5 Count 60")
    conf.set("spark.executor.memory", "5g")
    sc = SparkContext(conf=conf)

    tableindex = {"ORDERS": 3, "CANCELS": 4}
    tablechoice = argv[1]

    global start_time  # Using public var as params are not allowed(or I don't know of) to pass in the mapper function
    start_time = int(argv[2]) / 1000.0
    # start_time = 1349159961141 / 1000.0
    global end_time
    end_time = int(argv[3]) / 1000.0
    # end_time = 1349160643981 / 1000.0
    global interval
    interval = float(argv[4])
    # interval = 60.0
    global index
    index = tableindex[tablechoice]

    filepath = argv[5]

    # raw_file = sc.textFile("file:///shared_data//paths//f.csv")
    # rdd = raw_file.flatMap(add_date_then_query)

    add_date(filepath, start_time, end_time)
    raw_file = sc.textFile("file:///shared_data//paths//keys.txt")
    rdd = raw_file.flatMap(lambda x: import_hdf5(x, filepath))

    cc = rdd.count()
    print(cc)  # Check how much total data is there

    rdd = rdd.filter(lambda x: start_time <= x[index] / 1000.0 < end_time)  # Filter data according to the start and end times

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
