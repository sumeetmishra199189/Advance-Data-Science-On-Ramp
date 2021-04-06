#loading Fake_data.csv
data2='/Users/sumeetmishra/sw/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/Fake_data.csv'
input3=spark.read.csv(data2,,header=True)

#1) Find birth country which has highest amount of people
input4=input3.groupBy('Birth_Country').count()
input5=input4.orderBy('count',ascending = False)
input5.show(1)

#2) Find average income of people who are born in united states of america
from pyspark.sql.functions import col, avg
input3.filter(input3['Birth_Country'] == "United States of America").agg(avg(col("Income"))).show()

#3) How many people has income over 100,000 but their loan is not approved.

input3.filter(col("Loan_Approved") == "False").filter(col("Income") > 100000).count()

#4) Find top 10 people with highest income in United States of america. (Print their names, income and jobs)

from pyspark.sql.types import IntegerType
input3 = input3.withColumn("Income", input3["Income"].cast(IntegerType()))
input3.select("First_Name", "Income", "Job").filter(col("Birth_Country") == "United States of America").orderBy("Income",ascending=False).show(10)

#5) How many number of distinct jobs are there?

print(input3.select("Job").distinct().count())

#6) How many writers earn less than 100,000?

input3.filter(col("Job") == "Writer").filter(col("Income") < 100000).count()
