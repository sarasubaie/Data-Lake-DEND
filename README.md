
Project: Data Lake
Use Spark to run ELT processes and analytics on data of diverse sources, structure
Understand the components and issues of data lakes
*************************
The Koalas project makes data scientists more productive when interacting with big data
***********************************************************************************
Introduction:
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results

*********************************************

The Power of Spark
Understand the big data ecosystem

Project Datasets:
The data is queried from s3 buckets hosten at AWS

Song Dataset: s3://udacity-dend/song_data
Log Dataset:s3://udacity-dend/log_data

Fact Table:

songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables:

users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday


etl.py: Python script to extract the needed information from Song and Log data inside the S3 buckets and parsing/inserting them to the local directory

