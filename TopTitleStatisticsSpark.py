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
ans1 = ans2/count
sumSq = squares.reduce(lambda a,b: a+b)

var1 = lines.map(lambda a: a-ans1)
asn5 = var1.map(lambda a: a*a).reduce(lambda a,b: a+b)
asn5 = int(asn5/count)
#TODO

outputFile = open(sys.argv[2],"w")

outputFile.write('Mean\t%s\n' % int(ans1))
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % asn5)


sc.stop()

