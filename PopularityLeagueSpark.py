#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1], 1) 
leagueIds = sc.textFile(sys.argv[2], 1)
serie = sc.parallelize(list(range(0,leagueIds.count())))


#TODO (link, times), filter with league, sort, update value
lines = lines.map(lambda line: line.replace(':', ''))
linked = lines.flatMap(lambda line: line.split()[1:])
linked = linked.map(lambda link: (link, 1)).reduceByKey(lambda a,b: a+b)

leagueIds = leagueIds.map(lambda a: (a.strip('\n'),1))
linkedLeague = linked.join(leagueIds).map(lambda a: (a[0], a[1][0]))

linkedLeague = linkedLeague.sortBy(lambda a: a[1]).map(lambda a: a[0]).zipWithIndex()
linkedLeague = linkedLeague.sortByKey().map(lambda a: str(a[0]) + '\t' + a[1] + '\n')
result = linkedLeague.take(leagueIds.count())





#TODO

output = open(sys.argv[3], "w")

#TODO

for ele in result:	
	output.write(ele)
#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

