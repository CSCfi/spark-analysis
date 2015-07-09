import os
from os.path import dirname
import errno
from pyspark import SparkConf, SparkContext
from datetime import datetime, date, timedelta
import sys
from helper import testpath


def testmethod(x):

    # from helper import testpath
    a = os.path.dirname(os.path.abspath(__file__))
    b = __file__
    print(a)
    print(b)
    c = testpath(x)
    print(c)
    d = dirname(dirname(a))
    d = d + '/data/files/'
    print(d)

    try:
        os.makedirs(d + 'test')
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def main():

    conf = SparkConf()
    conf.setAppName("Application name")
    conf.set("spark.executor.memory", "5g")
    sc = SparkContext(conf=conf)

    sc.addFile("models.py")
    sc.addFile("runner.py")
    sc.addFile("helper.py")  # To import custom modules

    # c = testpath("Hello")
    # print(c)
    testmethod("Hello")

if __name__ == '__main__':
    main()
