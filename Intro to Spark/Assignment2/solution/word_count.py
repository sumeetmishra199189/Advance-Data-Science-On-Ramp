import sys
 
from pyspark import SparkContext, SparkConf
 
if __name__ == "__main__":
	
	# create Spark context with necessary configuration
	sc = SparkContext("local","PySpark Word Count Exmaple")
	#from pprint import pprint
	# read data from text file and split each line into words
	words = sc.textFile("/Users/sumeetmishra/sw/spark-2.4.4-bin-hadoop2.7/data/assignment_2_datafile.txt").flatMap(lambda line: line.split(" "))
	#result= sc.textFile("/Users/sumeetmishra/sw/spark-2.4.4-bin-hadoop2.7/data/word_count_output.txt")
	# count the occurrence of each word
	wordCounts = words.map(lambda word: (word, 1) if len(word)>=3 else (word,0)).reduceByKey(lambda a,b:a +b).map(lambda x:(x[1],x[0])).sortByKey(False)
	#wordCounts.collect()
	for word in wordCounts.collect():
		print(word)
		#word>>result
	#sc.parallelize(wordCounts).collect()
	wordCounts.rdd.saveAsTextFile("/Users/sumeetmishra/sw/spark-2.4.4-bin-hadoop2.7/data/word_count_output.txt")
	

	#pprint(wordCounts)
	#wordCounts.coalesce(1).write.format('text').save("/Users/sumeetmishra/sw/spark-2.4.4-bin-hadoop2.7/data/word_count_output.txt")
	#wordCounts.write.save("/Users/sumeetmishra/sw/spark-2.4.4-bin-hadoop2.7/data/word_count_output.txt")
	#### save the counts to output
	#wordCounts1.saveAsTextFile("/Users/sumeetmishra/sw/spark-2.4.4-bin-hadoop2.7/data/word_count_output.txt")
