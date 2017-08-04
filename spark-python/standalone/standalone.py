#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
import os
import sys


def activate_pyspark():
    # Detect existing SPARK_HOME. If it is set, retrieve it. Otherwise provide some fixed installation directory.
    if 'SPARK_HOME' in os.environ:
        SPARK_HOME = os.environ['SPARK_HOME']
    else:
        SPARK_HOME = '/usr/lib/spark'
        os.environ['SPARK_HOME'] = SPARK_HOME

    # Provide the correct Python version to Spark
    os.environ['PYSPARK_PYTHON'] = sys.executable

    # Add PySpark packages to the Python package path
    sys.path.append(os.path.join(SPARK_HOME, 'python'))
    sys.path.append(os.path.join(SPARK_HOME, 'python', 'lib', 'py4j-0.10.4-src.zip'))


activate_pyspark()

# Now it is possible to import the SparkSession class from PySpark
from pyspark.sql import SparkSession


class TestSpark(unittest.TestCase):
    def setUp(self):
        """
        This method is overriden from the base class and provides a place for setting up test fixtures. We use
        this place for creating a local SparkSession.
        """
        self.session = SparkSession.builder \
            .appName("test") \
            .master("local[2]") \
            .enableHiveSupport() \
            .getOrCreate()

    def tearDown(self):
        """
        This method is overriden from the base class and provides a place for tearing down any test fixtures. Here
        we stop the local SparkSession
        """
        self.session.stop()

    def test_spark(self):
        """
        This method contains one unit test case.
        """
        text = self.session.sparkContext.parallelize(["This is a test","yes really"])
        count = text.flatMap(lambda x: x.split()).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y).count()
        self.assertEqual(count, 6)


if __name__ == '__main__':
    unittest.main()
