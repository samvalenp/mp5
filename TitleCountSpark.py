#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
import re
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

stopWords = set()
stopWords.add('')
with open(stopWordsPath) as f:
	for linea in f:
		stopWords.add(linea.strip())

with open(delimitersPath) as f:
	delimiters = f.read()

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[3],1)

#START: tokenize, lowercase, stopwords

# tokenize
          #  	,;.?!-:@[](){}_*/
words1 = lines.flatMap(lambda line: re.split('['+re.escape(r'[\s\t,;\.\?!-:@\[\]\(\)\{\}_\*/]', line.rstrip())))

# lowercase
words2 = words1.map(lambda word: word.lower())
# stopwords
words3 = words2.filter(lambda word: word not in stopWords)
# count the occurrence of each word
wordCounts = words3.map(lambda word: (word, 1)).reduceByKey(lambda a,b: a+b)
# top 10
top = wordCounts.takeOrdered(5,lambda x: -x[1])
toprdd = sc.parallelize(top)
#END

# outputFile = open(sys.argv[4],"w")

#START
topOut = toprdd.sortByKey().map(lambda x: x[0]+ " " + str(x[1]))
topOut.saveAsTextFile(sys.argv[4])

#END
#write results to output file. Foramt for each line: (line +"\n")

sc.stop()
