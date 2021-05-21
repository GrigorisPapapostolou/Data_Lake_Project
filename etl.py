import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format 
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_date

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


###############################################################################
# - The Songs Table receives data  from the DataFrame of Songs.               #
# - The Artists Table receives data from the DataFrame of Songs.              #
###############################################################################

def process_song_data(spark, input_data, output_data):
    
    ############################################
    # 1. Import song json files to a DataFrame #
    ############################################    
    
    df_song = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))
    
    ###########################################
    # 2. Create temp table :  staging_songs   #
    ###########################################
    
    df_song.createOrReplaceTempView("staging_songs")

    ###########################################
    # 3. Create table :  songs                #
    ###########################################   
    
    songs_df= spark.sql ("""
        SELECT DISTINCT
            song_id     ,
            title       ,
            artist_id   ,
            duration    ,
            year
        FROM staging_songs
    """).collect()
    df1 = spark.createDataFrame(songs_df)
    df1.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, 'songs'))
    
    ###########################################
    # 3. Create table :  artist              #
    ###########################################
    
    artists_df= spark.sql ("""
        SELECT DISTINCT
            artist_id          AS artist_id ,
            artist_name        AS name      ,
            artist_location    AS location  ,
            artist_latitude    AS latitude  ,
            artist_longitude   AS longtitude
        FROM staging_songs  
    """).collect()
    df2 = spark.createDataFrame(artists_df)
    df2.write.parquet(os.path.join(output_data, 'artists'))
    
###############################################################################
# - The User Table receives data from the DataFrame of Logs .                 #
# - The Songplay Table receives data from the DataFrame of Logs and Songs.    #
# - The Time Table is created based on ts of the DataFrame of Logs .          #
###############################################################################

def process_log_data(spark, input_data, output_data):
    
    ###########################################
    # 1. Import log data files to a DataFrame #
    ###########################################
    
    # df_log = spark.read.json(os.path.join(input_data, "log_data/*/*/*.json"))
    df_log = spark.read.json(os.path.join(input_data, "log_data/*.json"))

    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)), IntegerType())
    df_log = df_log.withColumn('timestamp', get_timestamp(df_log.ts))
    df_log = df_log.withColumn('date', to_date("timestamp"))
    df_log = df_log.withColumn('start_time', date_format("timestamp", 'HH:mm:ss'))
    df_log = df_log.withColumn('hour', hour("timestamp"))
    df_log = df_log.withColumn('day', dayofmonth("date"))
    df_log = df_log.withColumn('week', weekofyear("date"))
    df_log = df_log.withColumn('month', month("date"))
    df_log = df_log.withColumn('year', year("date"))
    df_log = df_log.withColumn('weekday', date_format('date', 'E'))
    
    ###########################################
    # 2. Create temp table :  staging_events  #
    ###########################################
    
    df_log.createOrReplaceTempView("staging_events")
    
    ###########################################
    # 3. Create table :  users                #
    ###########################################
    
    user_df = spark.sql ("""
        SELECT DISTINCT
                userid       AS user_id    ,
                firstname    AS first_name ,
                lastname     AS last_name  ,
                gender       AS gender     ,  
                level        AS level
        FROM staging_events
        WHERE page = 'NextSong'
    """).collect()
    df3 = spark.createDataFrame(user_df)
    df3.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))
    
    ###########################################
    # 4. Create table :  time                 #
    ###########################################

    time_df= spark.sql ("""
        SELECT DISTINCT
            start_time,
            hour      , 
            day       ,
            week      , 
            month     ,
            year      , 
            weekday
        FROM staging_events
    """).collect()
    df4 = spark.createDataFrame(time_df)
    df4.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data, 'time'))
    
    ###########################################
    # 5. Create table :  songplays            #
    ###########################################

    songplays_df = spark.sql ("""
        SELECT DISTINCT
            staging_events.userId       AS user_id   ,
            staging_songs.song_id       AS song_id   ,
            staging_songs.artist_id     AS artist_id ,
            staging_events.start_time   AS startime  ,
            staging_events.level        AS level     ,
            staging_events.sessionId    AS session_id,
            staging_events.location     AS location  ,
            staging_events.userAgent    AS user_agent,
            staging_events.year         AS year      ,
            staging_events.month        AS month

        FROM staging_events          staging_events

        JOIN staging_songs           staging_songs
        ON   staging_events.artist = staging_songs.artist_name
        AND  staging_events.song   = staging_songs.title
        AND  staging_events.length = staging_songs.duration

        WHERE staging_events.page  = 'NextSong'
    """).collect()
    df5 = spark.createDataFrame(songplays_df)
    df5.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
  
    #input_data = "s3a://udacity-dend/"
    #output_data = "s3a://udacity-dend/"
    
    #process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)

    process_song_data(spark, "./song_data" , "./output_files")    
    process_log_data(spark, "./" , "./output_files")


if __name__ == "__main__":
    main()
