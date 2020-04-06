#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1],1)
lines = lines.map(lambda line: line.split("\t")).map(lambda line: int(line[1]))
squares = lines.map(lambda n: n*n)
ans4 = lines.reduce(lambda a,b: max(a,b))
ans3 = lines.reduce(lambda a,b: min(a,b))
ans2 = lines.reduce(lambda a,b: a+b)
count = lines.count()
ans1 = int(ans2/count)
sumSq = squares.reduce(lambda a,b: a+b)
ans5 = int(sumSq/count) - int(ans1*ans1)
#TODO

outputFile = open(sys.argv[2],"w")

outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)


sc.stop()

