#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO
linked = lines.flatMap(lambda line: line.split()[1:])
linked = linked.map(lambda link: (link, 1))

linking = lines.map(lambda line: line.split()[1])
linking = linking.map(lambda link: (link, 1))

together = linked.union(linking)
together = together.reduceByKey(lambda a,b: a+b)
orphan = together.filter(lambda x: x[1] == 0).sortByKey()
n = orphan.count()
result = orphan.map(lambda x: x[0]+'\n').take(n)

output = open(sys.argv[2], "w")

#TODO

for ele in result:	
	outputFile.write(ele+ '\n')
#write results to output file. Foramt for each line: (line+"\n")

sc.stop()

