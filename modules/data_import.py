from pyspark.sql import *
# from hdf5_map import date_query, import_hdf5
import h5py
from pyspark import SparkConf, SparkContext
from datetime import datetime, date, timedelta
import sys
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, LongType
import os
from os.path import dirname
from helper import saveDataset   # If you added a file in sc in above step then import it for usage


def add_all_dates(originalpath):

    d = dirname(dirname(os.path.abspath(__file__)))
    d = d + '/data/paths'
    with h5py.File(originalpath) as curr_file:
        with open(d + '/keys.txt', 'w') as hfile:
                filekeys = curr_file.keys()
                for k in filekeys:
                    print>>hfile, k


def sql(orders, sqlContext, originalpath, description, details):

    schemaString_orders = "id ref ob_id created destroyed side price quantity is_round past_id new_id p_id"

    fields_orders = []
    for field_name in schemaString_orders.split():
        if(field_name not in ['created', 'destroyed']):
            fields_orders.append(StructField(field_name, IntegerType(), True))
        else:
            fields_orders.append(StructField(field_name, LongType(), True))

    schema_orders = StructType(fields_orders)

    # Apply the schema to the RDD.
    schemaOrders = sqlContext.createDataFrame(orders, schema_orders)

    # schemaOrders.show()
    # schemaOrders.registerTempTable("orders")

    # results = sqlContext.sql("SELECT id, price, side FROM orders")
    # for (id, price, side) in results.collect():
    #    print(price)

    # schemaOrders.saveAsParquetFile("files/orders_name.parquet")
    saveDataset(schemaOrders, "orders", originalpath, description, details)


def cancels_sql(cancels, sqlContext, originalpath, description, details):

    schemaString_cancels = "id past_id new_id ob_id timestamp side price quantity"
    fields_cancels = []
    for field_name in schemaString_cancels.split():
        if(field_name not in ['timestamp']):
            fields_cancels.append(StructField(field_name, IntegerType(), True))
        else:
            fields_cancels.append(StructField(field_name, LongType(), True))

    schema_cancels = StructType(fields_cancels)
    schemaCancels = sqlContext.createDataFrame(cancels, schema_cancels)
    # schemaCancels.saveAsParquetFile("files/cancels_name.parquet")
    saveDataset(schemaCancels, "cancels", originalpath, description, details)


def trades_sql(trades, sqlContext, originalpath, description, details):

    schemaString_trades = "id ref o_id ob_id timestamp side quantity price p_id cp_id"
    fields_trades = []
    for field_name in schemaString_trades.split():
        if(field_name not in ['timestamp']):
            fields_trades.append(StructField(field_name, IntegerType(), True))
        else:
            fields_trades.append(StructField(field_name, LongType(), True))

    schema_trades = StructType(fields_trades)
    schemaTrades = sqlContext.createDataFrame(trades, schema_trades)
    # schemaTrades.saveAsParquetFile("files/trades_name.parquet")
    saveDataset(schemaTrades, "trades", originalpath, description, details)


def import_hdf5(x, originalpath, table):

    with h5py.File(originalpath) as f:
        data = f[str(x)].get(table)
        return list(data[:])


def to_int(x):
    i = 0
    while(i < len(x)):
        x[i] = int(x[i])
        i = i + 1
    return x


def main(argv):
    conf = SparkConf()
    conf.setAppName("Data Import")
    conf.set("spark.executor.memory", "5g")
    # conf.set("master", "spark://nandan-spark-cluster-fe:7077")
    sc = SparkContext(conf=conf)

    d = dirname(dirname(os.path.abspath(__file__)))
    print(d)
    d = d + '/data/paths'

    sc.addFile("helper.py")  # To import custom modules

    originalpaths = str(argv[0])
    originalpaths = originalpaths.split(',')

    description = str(argv[1])
    details = str(argv[2])

    for originalpath in originalpaths:
        add_all_dates(originalpath)
        raw_file = sc.textFile(d + "/keys.txt")

        rdd1 = raw_file.flatMap(lambda x: import_hdf5(x, originalpath, "ORDERS"))
        rdd1 = rdd1.map(lambda x: list(x))
        rdd1 = rdd1.map(to_int)

        rdd2 = raw_file.flatMap(lambda x: import_hdf5(x, originalpath, "CANCELS"))
        rdd2 = rdd2.map(lambda x: list(x))
        rdd2 = rdd2.map(to_int)

        rdd3 = raw_file.flatMap(lambda x: import_hdf5(x, originalpath, "TRADES"))
        rdd3 = rdd3.map(lambda x: list(x))
        rdd3 = rdd3.map(to_int)

        # d = rdd.count()
        # print(d)

        sqlContext = SQLContext(sc)
        sql(rdd1, sqlContext, originalpath, description, details)
        cancels_sql(rdd2, sqlContext, originalpath, description, details)
        trades_sql(rdd3, sqlContext, originalpath, description, details)


if __name__ == '__main__':
    main(sys.argv[1:])
