# Assignment-3

This homework is based on module "Importing data into Spark"

Homework Data

Locate the file people.json in your spark installation directory at the path "../../examples/src/main/resources/people.json"

To-Do

1. Import the data from people.json file using JSON python library. 

( hint : refer section 1-2 Loading JSON )

2. Import people.json file using HiveContext or sqlContext into a DataFrame. Print schema information using printSchema() function. Next register the dataframe as a temporary table. Display distinct names from the people.json file by firing a SQL query on the temporary table created. Submit your code and the screenshot of the results.

( hint : refer section "Structured data with SPARK SQL" )

3. Import file people.txt located at the same location as people.json into a rdd using built-in CSV library. Import the CSV library at the beginning and use csv.reader() to load the data. Display the data in your rdd. Submit your code and screenshot of the results.

( hint : refer csv.reader )

 

What to turn in?

1. Python code files

2. PDF files containing your answers or screenshots

Optional

1. There are various ways to load JSON and CSV files based on the version of Spark installed. Search for new import/export libraries and try loading larger files for analyzing the data using functions discussed in previous modules.

2. Try importing data into spark from Hadoop DFS, HBase, MongoDB, Cassandra, SQL Databases, Amazon S3 and Elastic Search if you have access to any of these data sources. If you got any other data stores or storage technologies, check if a Spark connector exists through which you can load data into Spark.