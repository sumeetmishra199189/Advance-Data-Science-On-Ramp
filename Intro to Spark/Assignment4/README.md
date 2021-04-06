# Assignment-4

This assignment is about Spark Transformations & Actions.

We have generated some fake data of people in following format:

Birth_Country	Email	First_Name	Income	Job	Last_name	Loan_Approved	SSN
You will often have to work with this kind of tabular data for many problems. So here, your task is to answer following questions using spark transformations & actions and also by using spark SQL.

0) Load fake_data.csv(check canvas files section) into spark dataframe

Hint 1: Please refer spark csv document to understand how to load csv files directly to dataframe.

Hint 2: You can also read csv files by first reading as a text file and then splitting lines by "," delimiter.

1) Find birth country which has highest amount of people

2) Find average income of people who are born in united states of america

3) How many people has income over 100,000 but their loan is not approved.

4) Find top 10 people with highest income in United States of america. (Print their names, income and jobs)

5) How many number of distinct jobs are there?

6) How many writers earn less than 100,000? 

 

What to turn in?

1) Python code file with transformation and actions approach

2) Python code file with Spark SQL approach

3) PDF files containing your answers