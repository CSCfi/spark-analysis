from pyspark.sql import *
import h5py
from pyspark import SparkConf, SparkContext
from datetime import datetime, date, timedelta
import sys
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, LongType
import os
from os.path import dirname
from tempfile import NamedTemporaryFile
from utils.helper import saveDataset   # If you added a file in sc in above step then import it for usage


def add_all_dates(originalpath):

    hfile = NamedTemporaryFile(delete=False, dir='/shared_data/tmp')
    with h5py.File(originalpath) as curr_file:
            filekeys = curr_file.keys()
            for k in filekeys:
                print>>hfile, k
    hfile.close()
    return hfile


def orders_sql(configpath, orders, sqlContext, userdatadir, originalpath, description, details):

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

    saveDataset(configpath, schemaOrders, userdatadir, "orders", originalpath, description, details)


def cancels_sql(configpath, cancels, sqlContext, userdatadir, originalpath, description, details):

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
    saveDataset(configpath, schemaCancels, userdatadir, "cancels", originalpath, description, details)


def trades_sql(configpath, trades, sqlContext, userdatadir, originalpath, description, details):

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
    saveDataset(configpath, schemaTrades, userdatadir, "trades", originalpath, description, details)


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
    conf.set("spark.jars", "file:/shared_data/spark_jars/hadoop-openstack-3.0.0-SNAPSHOT.jar")
    sc = SparkContext(conf=conf)

    helperpath = dirname(os.path.abspath(__file__))
    sc.addFile(helperpath + "/utils/helper.py")  # To import custom modules

    originalpaths = str(argv[0])
    originalpaths = originalpaths.split('&')

    description = str(argv[1])
    details = str(argv[2])

    userdatadir = str(argv[3])
    configpath = str(argv[4])

    for originalpath in originalpaths:
        hfile = add_all_dates(originalpath)
        # print(hfile.name)
        # print(d + '/keys.txt')

        raw_file = sc.textFile('file://' + hfile.name)
        rdd1 = raw_file.flatMap(lambda x: import_hdf5(x, originalpath, "ORDERS"))
        rdd1 = rdd1.map(lambda x: list(x))
        rdd1 = rdd1.map(to_int)

        rdd2 = raw_file.flatMap(lambda x: import_hdf5(x, originalpath, "CANCELS"))
        rdd2 = rdd2.map(lambda x: list(x))
        rdd2 = rdd2.map(to_int)

        rdd3 = raw_file.flatMap(lambda x: import_hdf5(x, originalpath, "TRADES"))
        rdd3 = rdd3.map(lambda x: list(x))
        rdd3 = rdd3.map(to_int)

        # d = rdd1.count()
        # print(d)

        sqlContext = SQLContext(sc)
        orders_sql(configpath, rdd1, sqlContext, userdatadir, originalpath, description, details)
        cancels_sql(configpath, rdd2, sqlContext, userdatadir, originalpath, description, details)
        trades_sql(configpath, rdd3, sqlContext, userdatadir, originalpath, description, details)
        os.unlink(hfile.name)


if __name__ == '__main__':
    main(sys.argv[1:])
