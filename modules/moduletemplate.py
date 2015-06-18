from pyspark import SparkConf, SparkContext
from datetime import datetime, date, timedelta
import sys
from operator import add
from pyspark.sql import *
# from helper import saveFeatures   # If you added a file in sc (in main) then import it for usage


# A General dummy function to be used in Map , it transforms the epoch time into human readable format
# def timetr(x):
#    dt = datetime.fromtimestamp(x.timefield).strftime('%Y-%m-%d %H:%M:%S.%f')
#    return (dt, x)


def your_module_implementation(sc, params, inputs):

    param1 = int(params[0])  # First convert all the parameters with their proper types
    param2 = str(params[1])
    # param3 = float(params['param3']
    # param4 = long(params['param4'])

    inputfile = inputs[0]  # As if now it is expected that each module will run on a single dataset
    # inputfile2 = inputs[1]  #But it is possible to use another file and create dataframe for it and apply joining

    sqlContext = SQLContext(sc)  # Create SQLContext var from SparkContext, Useful only if you are using dataframes i.e. parquet files
    rdd = sqlContext.parquetFile(inputfile)
    rdd.registerTempTable("tablename")  # Register the table in parquet file for our usage with SQL queries
    rdd = sqlContext.sql("SELECT fields from tablename")  # A dataframe is returned which can be used as an RDD

    # rdd1 = rdd.filter(rdd.somefieldname == somecriteria)  # Example of filtering
    # rdd1 = rdd1.map(somemapfunction)  # Applying transformation to the RDD by passing custom functions to map
    # rdd1 = rdd1.reduceByKey(somereducefunction)  # For performing sum or average of the results
    # rdd1 = rdd1.groupByKey()  # Useful for grouping the results by key
    # rdd1 = rdd1.map(timetr)  # If you need human readable time

    # d = rdd.collect()  #Collect the results before displaying
    # for k in d:  # Print out the results
    #    print(k)

    # For saving the results to a parquet file
    # Convert the RDD to a dataframe first by defining the schema

    p = re.compile('.+/(\w+)\.\w+')
    modulename = p.match(__file__).group(1)
    params_used = ','.join(params)
    saveFeatures(dataframe, "feature-dataset-name", "short description", "details", modulename, params_used, inputs)
    sc.stop()


def main(args):

    params = str(args[1]).split(,)
    inputs = str(args[2]).split(,)

    # Configure Spark
    conf = SparkConf()
    conf.setAppName("Application name")
    conf.set("spark.executor.memory", "5g")
    sc = SparkContext(conf=conf)  # Spark Context variable that will be used for all operations running on the cluster

    # sc.addFile("/path/to/helper.py")  # To import custom modules

    your_module_implementation(sc, params, inputs)

if __name__ == "__main__":
    main(sys.argv)
