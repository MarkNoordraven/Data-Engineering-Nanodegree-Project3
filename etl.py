import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
import pyspark.sql.functions as f

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Instantiates a spark session
    """  
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """ Reads song data and writes songs and artists tables to S3

        Returns
        ----------
        spark : psycopg2 connection cursor with type pyspark.sql.session.SparkSession
        input data: AWS S3 folder with type string consisting of song jsons 
        output data: target AWS S3 folder with type string which the Sparkify team can query
    """      
    spark.sparkContext._jsc.hadoopConfiguration()\
         .set("mapreduce.fileoutputcommitter.algorithm.version", "2") # speeds up S3 writes

    song_data = input_data +'song_data/*/*/*/*.json'

    df = spark.read.json(song_data)

    songs_table = df.select("song_id", 'title', "artist_id", "year", "duration")\
                    .drop_duplicates(subset = ['song_id'])
    
    songs_table.write.mode('overwrite')\
               .partitionBy('year', 'artist_id').\
                parquet(output_data + "songs",
                        'overwrite')
    
    artists_table = df.select('artist_id', 
                              'artist_name', 
                              'artist_location', 
                              'artist_latitude', 
                              'artist_longitude')\
                      .drop_duplicates(subset = ['artist_id'])

    artists_table.write.mode('overwrite')\
                       .partitionBy('artist_name')\
                       .parquet(output_data + 'artists',
                                'overwrite')


def process_log_data(spark, input_data, output_data):
    """ Reads song data and log data and writes users, time and songplays tables to S3

        Returns
        ----------
        spark : psycopg2 connection cursor with type pyspark.sql.session.SparkSession
        input data: AWS S3 folder with type string consisting of song and log jsons
        output data: target AWS S3 folder with type string which the Sparkify team can query
    """
    spark.sparkContext._jsc.hadoopConfiguration()\
         .set("mapreduce.fileoutputcommitter.algorithm.version", "2") # speeds up S3 writes
    
    log_data = input_data +'log_data/*/*/*.json'

    df_log = spark.read.json(log_data)

    df_filtered = df_log.where("page = 'NextSong'")

    users_table = df_filtered.select('userId',
                                     'firstName',
                                     'lastName',
                                     'gender',
                                     'level')\
                             .drop_duplicates(subset = ['userId'])
    
    users_table.write.mode('overwrite')\
                     .partitionBy('userId')\
                     .parquet(output_data + 'users',
                              'overwrite')

    df_time = df_filtered.withColumn("timestamp", 
                                     from_unixtime(df_filtered['ts']/1000,"yyyy-MM-dd'T'HH:mm:ss.SSS"))
    
    df_time = df_time.withColumn("datetime", 
                                 from_unixtime(df_filtered['ts']/1000, "dd/MM/yyyy HH:MM:SS"))
    
    df_time = df_time.withColumn('hour', hour('timestamp'))
    df_time = df_time.withColumn('day', dayofmonth('timestamp'))
    df_time = df_time.withColumn('week', weekofyear('timestamp'))
    df_time = df_time.withColumn('month', month('timestamp'))
    df_time = df_time.withColumn('year', year('timestamp'))
    df_time = df_time.withColumn('weekday', dayofweek('timestamp'))
    time_table = df_time.select('datetime', 'hour', 'day', 'week', 'month', 'year', 'weekday')\
                        .drop_duplicates(subset=['datetime'])
    
    time_table.write.mode('overwrite')\
              .partitionBy('year', 'month')\
              .parquet(output_data + "time",
                       'overwrite')
    
    df = spark.read.json(input_data +'song_data/*/*/*/*.json')
    songs_table = df.select("song_id",
                            'title',
                            "artist_id",
                            "year",
                            "duration")\
                    .drop_duplicates(subset = ['song_id'])

    df_time = df_time.withColumn('songplay_id', monotonically_increasing_id()) # used as id column
    df_time_incl_artist = df_time.join(df.select('artist_id', 
                                                  'artist_name')\
                                         .drop_duplicates(subset = ['artist_id']),
                                      (df_time.artist == df.artist_name),
                                      'left_outer') # pre-joins the logs table to prevent SQL multi-joins

    df_time_incl_artist.createOrReplaceTempView("logs")
    songs_table.createOrReplaceTempView("songs_table")
    songplays_table = spark.sql("""
                                SELECT songplay_id,
                                       datetime as start_time,
                                       userId as user_id,
                                       level,
                                       song_id, 
                                       songs_table.artist_id,
                                       sessionId as session_id,
                                       location, 
                                       userAgent as user_agent,
                                       logs.year,
                                       logs.month
                                FROM logs
                                JOIN songs_table ON (logs.song = songs_table.title 
                                                     AND logs.artist_id = songs_table.artist_id
                                                     AND logs.length == songs_table.duration)
                                """)
    
    songplays_table.write.mode('overwrite')\
                   .partitionBy('year', 'month')\
                   .parquet(output_data + "songplays",
                            'overwrite')

def main():
    """ Creates a spark session, and reads song data and log data 
        and writes users, time, songs, artsists and songplays tables to S3
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakebucket5/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
