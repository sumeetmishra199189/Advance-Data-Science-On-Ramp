import csv
data2='/Users/sumeetmishra/sw/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.txt'
input3 =spark.read.csv(data2)
print(input3.collect())
