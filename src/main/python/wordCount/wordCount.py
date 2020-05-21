from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession

from pyspark.context import SparkContext, SparkConf

if __name__ == "__main__":
    config = SparkConf().setAppName("wordCount").setMaster("local")
    sc = SparkContext()
    lines = sc.textFile("./src\main\python\wordCount\hello.txt")
    words = lines.flatMap(lambda line: line.split(" "))
    wordCountMap = words.map(lambda word:(word, 1))
    # count = wordCountMap.reduceByKey(lambda preCount, count: preCount + count)
    # output = count.collect()
    # print(output) 

    # use countByKey instead
    count = wordCountMap.countByKey()
    print(count)