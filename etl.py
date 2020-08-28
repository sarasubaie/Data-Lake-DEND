import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import databricks.koalas as ks
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "data/song_data/A/B/C/*.json"
    kdf = ks.read_json("data/song_data/A/B/C/*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    
    songs_table = (ks.sql('''
                   SELECT 
                   DISTINCT
                   row_number() over (ORDER BY year,title,artist_id) id,
                   title,
                   artist_id,
                   year,
                  duration
                   FROM 
                       {kdf}''')
                  )

    songs_table
    
    # write songs table to parquet files partitioned by year and artist
    (songs_table
     .to_spark()
     .write
     .partitionBy("year", "artist_id")
     .parquet('songs/')
    )
     
    # extract columns to create artists table
    artists_table = (ks.sql('''
               SELECT 
               DISTINCT
               artist_id,
               artist_name,
               artist_location,
               artist_latitude,
               artist_longitude
               
               FROM 
                   {kdf}''')
              )

    artists_table
    
    # write artists table to parquet files
    (artists_table
     .to_spark()
     .write
     .parquet('artists/')
    )

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =("data/log_data/*.json")
    kdf_logs = ks.read_json("data/log_data/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df= df[df["page"]=="NextSong"]
    # extract columns for users table    
    users_table = (ks.sql('''
               SELECT 
               DISTINCT
               userId,
               firstName,
               lastName,
               gender,
               level
               FROM 
                   {kdfLog}''')
              )
    users_table.head()
    # write users table to parquet files
    (users_table
     .to_spark()
     .write
     .parquet('users_table/')
    )

    # create timestamp column from original timestamp column
   ## get_timestamp = udf()
    ##df = 
    ts = ks.to_datetime(kdfLog.ts, unit='ms') 

    
    # create datetime column from original timestamp column
   # get_datetime = udf()
   # df = 
    # extract columns to create time table
    time_table = (ks.sql('''
                SELECT
                ts as start_time,
                HOUR(ts) as hour,
                DAY(ts) as day,
                EXTRACT(week FROM ts) as week,
                MONTH(ts) as month,
                YEAR(ts) as year,
                WEEKDAY(ts) as weekday
                FROM
                    {ts}
                ''')
              )
    # write time table to parquet files partitioned by year and month
    (time_table
      .to_spark()
      .write
      .partitionBy("year","month")
      .parquet('time_table/')
    )

    # read in song data to use for songplays table
    song_data = 'data/song_data/A/A/A/*.json'
    song_df = ks.read_json(song_data)
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (ks.sql('''
                SELECT
                row_number() over (ORDER BY userId) AS songplay_id,
                to_timestamp(ts / 1000) AS start_time,
                YEAR(to_timestamp(ts / 1000)) AS year,
                MONTH(to_timestamp(ts / 1000)) AS month,
                userId AS user_id,
                dfl.level,
                sdf.song_id,
                sdf.artist_id,
                sessionId AS session_id,
                location,
                userAgent AS user_agent
                FROM {kdfLog} dfl JOIN {song_df} sdf
                ON dfl.artist = sdf.artist_name
                WHERE page = 'NextSong' 
                ''')
                )
    song_play_table
    # write songplays table to parquet files partitioned by year and month
    (song_play_table
       .to_spark()
       .write
       .partitionBy("year","month")
       .parquet('song_play_table/')
    )





def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
