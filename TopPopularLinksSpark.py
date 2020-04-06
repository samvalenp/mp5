#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO
ntitles = 10

lines = lines.map(lambda line: line.replace(':', ''))
linked = lines.flatMap(lambda line: line.split()[1:])
linked = linked.map(lambda link: (link, 1)).reduceByKey(lambda a,b: a+b)

top = linked.takeOrdered(ntitles,lambda x: -x[1])
toprdd = sc.parallelize(top)

output = open(sys.argv[2], "w")

#TODO
topOut = toprdd.sortByKey().map(lambda x: str(x[0])+ "\t" + str(x[1]))
result = topOut.take(ntitles)
for ele in result:	
	output.write(ele+ '\n')
#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

