import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = '{}/song_data/*/*/*/*.json'.format(input_data)
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
                        'song_id',
                        'title',
                        'artist_id',
                        'year',
                        'duration'
                    ).dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(
                        'year',
                        'artist_id'
                    ).parquet(
                        os.path.join(
                            output_data,
                            'songs/songs.parquet'
                        ),
                        'overwrite'
                    )

    # extract columns to create artists table
    artists_table = df.select(
                        'artist_id',
                        'artist_name',
                        'artist_location',
                        'artist_latitude',
                        'artist_longitude'
                    ).withColumnRenamed(
                        'artist_name',
                        'name'
                    ).withColumnRenamed(
                        'artist_location',
                        'location'
                    ).withColumnRenamed(
                        'artist_latitude',
                        'latitude'
                    ).withColumnRenamed(
                        'artist_longitude',
                        'longitude'
                    ).dropDuplicates()
    artists_table.createOrReplaceTempView('artists') 
    
    # write artists table to parquet files
    artists_table.write.parquet(
                            os.path.join(
                                output_data,
                                'artists/artists.parquet'
                            ),
                            'overwrite'
                        )


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = '{}/log_data/*.json'.format(input_data)

    # read log data file
    df_all = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df_all.filter(df_all.page == 'NextSong').select(
                                            'ts',
                                            'userId',
                                            'level',
                                            'song',
                                            'artist',
                                            'sessionId',
                                            'location',
                                            'userAgent'
                                        )

    # extract columns for users table    
    users_table = df_all.select(
                        'userId',
                        'firstName',
                        'lastName',
                        'gender',
                        'level'
                    ).dropDuplicates()
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(
                            os.path.join(
                                output_data,
                                'users/users.parquet'
                            ),
                            'overwrite'
                        )

    # create timestamp column from original timestamp column
    get_timestamp = get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
                        'datetime'
                    ).withColumn(
                        'start_time',
                        df.datetime
                    ).withColumn(
                        'hour',
                        hour('datetime')
                    ).withColumn(
                        'day',
                        dayofmonth('datetime')
                    ).withColumn(
                        'week',
                        weekofyear('datetime')
                    ).withColumn(
                        'month',
                        month('datetime')
                    ).withColumn(
                        'year',
                        year('datetime')
                    ).withColumn(
                        'weekday',
                        dayofweek('datetime')
                    ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(
                        'year',
                        'month'
                    ).parquet(
                        os.path.join(
                                    output_data,
                                    'time/time.parquet'
                        ), 
                        'overwrite'
                    )

    # read in song data to use for songplays table
    song_df = spark.read.json(
                    '{}/song_data/*/*/*/*.json'.format(input_data)
    )

    # extract columns from joined song and log datasets to create songplays table
    df = df.alias('log_df')
    song_df = song_df.alias('song_df')
    # joined_df = df.join(
    #             song_df, 
    #             col('log_df.artist') == col('song_df.artist_name'),
    #             'inner'
    #         )
    songplays_table = df.join(
                        song_df, 
                        col('log_df.artist') == col('song_df.artist_name'),
                        'inner'
                    ).select(
                        col('log_df.datetime').alias('start_time'),
                        col('log_df.userId').alias('user_id'),
                        col('log_df.level').alias('level'),
                        col('song_df.song_id').alias('song_id'),
                        col('song_df.artist_id').alias('artist_id'),
                        col('log_df.sessionId').alias('session_id'),
                        col('log_df.location').alias('location'), 
                        col('log_df.userAgent').alias('user_agent'),
                        year('log_df.datetime').alias('year'),
                        month('log_df.datetime').alias('month')
                    ).withColumn(
                        'songplay_id',
                        monotonically_increasing_id()
                    )
    songplays_table.createOrReplaceTempView('songplays')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(
        'year', 'month').parquet(os.path.join(output_data,
                                 'songplays/songplays.parquet'),
                                 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
