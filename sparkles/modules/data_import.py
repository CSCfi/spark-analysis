from pyspark.sql import SQLContext
import h5py
from pyspark import SparkConf, SparkContext
from datetime import datetime, date, timedelta
import sys
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, LongType
import os
from os.path import dirname
from tempfile import NamedTemporaryFile
from utils.helper import saveDataset   # If you added a file in sc in above step then import it for usage
import json
import argparse


def add_all_dates(configstr, originalpath):

    sparkles_tmp_dir = configstr['SPARKLES_TMP_DIR']
    hfile = NamedTemporaryFile(delete=False, dir=sparkles_tmp_dir)
    with h5py.File(originalpath) as curr_file:
            filekeys = curr_file.keys()
            for k in filekeys:
                print>>hfile, k
    hfile.close()
    return hfile


def orders_sql(configstr, orders, sqlContext, userdatadir, originalpath, description, details):

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

    saveDataset(configstr, schemaOrders, userdatadir, "orders", originalpath, description, details)


def cancels_sql(configstr, cancels, sqlContext, userdatadir, originalpath, description, details):

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
    saveDataset(configstr, schemaCancels, userdatadir, "cancels", originalpath, description, details)


def trades_sql(configstr, trades, sqlContext, userdatadir, originalpath, description, details):

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
    saveDataset(configstr, schemaTrades, userdatadir, "trades", originalpath, description, details)


def import_hdf5(x, originalpath, table):

    with h5py.File(originalpath) as f:
        data = f[str(x)].get(table)
        return list(data[:])


def numpy_to_native(x):

    return x.tolist()


def main():
    conf = SparkConf()
    conf.setAppName("Data Import")
    conf.set("spark.jars", "file:/shared_data/spark_jars/hadoop-openstack-3.0.0-SNAPSHOT.jar")
    sc = SparkContext(conf=conf)

    parser = argparse.ArgumentParser()
    parser.add_argument("backend", type=str)
    parser.add_argument("originalpaths", type=str)
    parser.add_argument("description", type=str)
    parser.add_argument("details", type=str)
    parser.add_argument("userdatadir", type=str)
    parser.add_argument("configstr", type=str)
    parser.add_argument("partitions", type=int)

    args = parser.parse_args()

    # Swift Connection
    if(args.backend == 'swift'):
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

    partitions = args.partitions  # Default number of jobs
    helperpath = dirname(os.path.abspath(__file__))
    sc.addFile(helperpath + "/utils/helper.py")  # To import custom modules

    originalpaths = json.loads(args.originalpaths)
    description = args.description
    details = args.details

    userdatadir = args.userdatadir
    configstr = args.configstr

    for originalpath in originalpaths:
        hfile = add_all_dates(configstr, originalpath)

        raw_file = sc.textFile('file://' + hfile.name, partitions)
        rdd1 = raw_file.flatMap(lambda x: import_hdf5(x, originalpath, "ORDERS"))
        rdd1 = rdd1.map(numpy_to_native)

        rdd2 = raw_file.flatMap(lambda x: import_hdf5(x, originalpath, "CANCELS"))
        rdd2 = rdd2.map(numpy_to_native)

        rdd3 = raw_file.flatMap(lambda x: import_hdf5(x, originalpath, "TRADES"))
        rdd3 = rdd3.map(numpy_to_native)

        # print(rdd1.count())

        sqlContext = SQLContext(sc)
        orders_sql(configstr, rdd1, sqlContext, userdatadir, originalpath, description, details)
        cancels_sql(configstr, rdd2, sqlContext, userdatadir, originalpath, description, details)
        trades_sql(configstr, rdd3, sqlContext, userdatadir, originalpath, description, details)
        os.unlink(hfile.name)


if __name__ == '__main__':
    main()
