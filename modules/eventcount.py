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


# Hack for using HDF5 datasets in Spark, also fetches the data from dataset using the dates provided by user
def add_date_query(x):
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


# Hash the keys into different interval periods
def keymod(x):

    curr_t = x[index] / 1000
    curr_t = curr_t - start_time
    keyindex = curr_t / interval
    return (keyindex, 1)


# Transform the final time
def timetr(x):

    dt = datetime.fromtimestamp(start_time + x[0] * interval).strftime('%Y-%m-%d %H:%M:%S')
    return (dt, x[1])


def main(argv):
    conf = SparkConf()
    # conf.setMaster("spark://nandan-spark-cluster-fe:6066")
    # conf.setMaster("local")
    conf.setAppName("H5 Test 4")
    # conf.set("spark.executor.memory", "5g")
    sc = SparkContext(conf=conf)

    tableindex = {"ORDERS": 3, "CANCELS": 4}
    tablechoice = argv[1]

    global start_time  # Using public var as params are not allowed(or I don't know of) to pass in the mapper function
    start_time = int(argv[2]) / 1000
    # start_time = 1349159961141 / 1000
    global end_time
    end_time = int(argv[3]) / 1000
    # end_time = 1349160643981 / 1000
    global interval
    interval = int(argv[4])
    # interval = 60
    global index
    index = tableindex[tablechoice]

    raw_file = sc.textFile("file:///shared_data//paths//f.csv")
    rdd = raw_file.flatMap(addDateThenQuery)

    # file_paths = sc.textFile("file:///shared_data/filenames1.csv")
    # rdd = file_paths.flatMap(fu)

    cc = rdd.count()
    print(cc)  # Check how much total data is there

    rdd = rdd.filter(lambda x: start_time <= x[index] / 1000 < end_time)  # Filter data according to the start and end times

    rdd1 = rdd.map(keymod).reduceByKey(add)
    rdd1 = rdd1.map(timetr)  # Human readable time
    d = rdd1.collect()

    for k in d:
        print(k)
        # print(v)

    sc.stop()


if __name__ == "__main__":
    main(sys.argv)
