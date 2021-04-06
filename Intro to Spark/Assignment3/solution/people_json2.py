import json
data='/Users/sumeetmishra/sw/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.json'
input2 =sqlContext.read.json(data)
input2.printSchema()
input2.createOrReplaceTempView('people_json)'
input2df=spark.sql('select distinct name from people_json')
input2df.show()
