
## Project: Data Modeling with Postgres
Third project in the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027)


![https://confirm.udacity.com/PLQJPKUN](https://github.com/MarkNoordraven/Data-Engineering-Nanodegree-CapstoneProject/blob/master/Data%20Engineering%20certificate.PNG)



#### Introduction
A startup called Sparkify wants to analyze their data of songs and user activity on their music streaming app.

The goal is to move their data warehouse to a data lake. In order to fulfill this, a data pipeline which loads JSON song and log data, turns these into fact and dimensional tables, and writes these back to S3.


Song metadata consists of the following fields: 

`artist_id`, `artist_latitude`, `artist_location`, `artist_longitude`, `artist_name`, `duration`, `num_songs`, `song_id title`, `year`

The user activity logs contain the following fields: 

`artist`, `auth`, `firstName`, `gender`, `itemInSession`, `lastName`, `length`, `level`, `location`, `method`, `page`, `registration`, `sessionId`, `song`, `status`, `ts`, `userAgent`, `userId`


#### Database schema
Simply creating two databases based on metadata makes it difficult to perform analysis, for instance:

- the activity logs only contain timestamp as an integer for the time dimension
- the song metadata doesn't contain song names


We opted to go for a star schema with fact table *songplays* and dimension tables *users*, *songs*, *artists*, *time* as this allows the analytics team
to slice and dice the data for different types of queries.

#### ETL Pipeline

- AWS credentials are securely stores in dl.cfg

- The load, ETL and write processes are perfoemd in etl.py

##### etl.py consists of the following functions:

- create_spark_session: instantiates a spark session hadoop-aws version 2.7.5  

- process_song_data: reads a spark session and input and ouput S3 folders, then filters and extracts the records, and inserts them into tables, which are written to partitioned parquet files in S3

- process_log_data: reads a spark session and input and ouput S3 folders, then filters and extracts the records, and inserts them into tables, which are written to partitioned parquet files in S3

- main: creates spark session, then runs process_song_data and process_log_data for the song and log jsons.

By modularizing the code in this way we can easily accomodate for changing business requirements such as new types of log files.
