
from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession

from pyspark.context import SparkContext, SparkConf

if __name__ == "__main__":
    spark = SparkSession.builder.appName("wordCount").master("local").getOrCreate()
    lines = spark.read.text("./src/main/python/wordCount/hello.txt").rdd.map(lambda row: row[0])
    words = lines.flatMap(lambda line: line.split(" "))
    counts = words.map(lambda word: (word, 1)).reduceByKey(add)
    print(counts.collect())