import json
data='/Users/sumeetmishra/sw/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.json'
input =spark.read.json(data)
input.printSchema()
