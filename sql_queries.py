import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = 'DROP TABLE IF EXISTS staging_events'
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_songs'
songplay_table_drop = 'DROP TABLE IF EXISTS songplays'
user_table_drop = 'DROP TABLE IF EXISTS users'
song_table_drop = 'DROP TABLE IF EXISTS songs'
artist_table_drop = 'DROP TABLE IF EXISTS artists'
time_table_drop = 'DROP TABLE IF EXISTS time'

# CREATE TABLES
staging_events_table_create = ('''
CREATE TABLE staging_events (
    event_id INT IDENTITY(0,1)
    ,artist VARCHAR(255)
    ,auth VARCHAR(255)
    ,first_name VARCHAR(255)
    ,gender VARCHAR(31)
    ,item_in_session INT
    ,last_name VARCHAR(255)
    ,length INT
    ,level VARCHAR(31)
    ,location VARCHAR(255)
    ,page VARCHAR(63)
    ,registration VARCHAR(63)
    ,session_id BIGINT
    ,song VARCHAR(255)
    ,status INT
    ,ts BIGINT
    ,user_agent TEXT
    ,user_id INT
    ,PRIMARY KEY (event_id)
)
''')

staging_songs_table_create = ('''
CREATE TABLE staging_songs (
    song_id VARCHAR(31)
    ,num_songs INT
    ,artist_id VARCHAR(31)
    ,artist_latitude DOUBLE PRECISION
    ,artist_longitude DOUBLE PRECISION
    ,artist_location VARCHAR(255)
    ,artist_name VARCHAR(255)
    ,title VARCHAR(255)
    ,duration DOUBLE PRECISION
    ,year INT
    ,PRIMARY KEY (song_id)
)
''')

songplay_table_create = ('''
CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1)
    ,start_time TIMESTAMP references time(start_time)
    ,user_id INT references users(user_id)
    ,level VARCHAR(31)
    ,song_id VARCHAR(31) references songs(song_id)
    ,artist_id VARCHAR(31) references artists(artist_id)
    -- Anticipating many plays
    ,session_id BIGINT
    ,location VARCHAR(255)
    -- Use a large placeholder since unknown length
    ,user_agent TEXT
    ,PRIMARY KEY (songplay_id)
)
''')

user_table_create = ('''
CREATE TABLE users (
    user_id INT
    ,first_name VARCHAR(255)
    ,last_name VARCHAR(255)
    ,gender VARCHAR(31)
    ,level VARCHAR(31)
    ,PRIMARY KEY (user_id)
)
''')

song_table_create = ('''
CREATE TABLE songs (
    song_id VARCHAR(31)
    ,title VARCHAR(255)
    ,artist_id VARCHAR(31)
    ,year INT
    ,duration DOUBLE PRECISION
    ,PRIMARY KEY (song_id)
)
''')

artist_table_create = ('''
CREATE TABLE artists (
    artist_id VARCHAR(31)
    ,name VARCHAR(255)
    ,location VARCHAR(255)
    ,latitude DOUBLE PRECISION
    ,longitude DOUBLE PRECISION
    ,PRIMARY KEY (artist_id)
)
''')

time_table_create = ('''
CREATE TABLE time (
    start_time TIMESTAMP
    ,hour INT
    ,day INT
    ,week INT
    ,month INT
    ,year INT
    ,weekday INT
    ,PRIMARY KEY (start_time)
)
''')

# STAGING TABLES

staging_events_copy = ('''
''').format()

staging_songs_copy = ('''
''').format()

# FINAL TABLES

songplay_table_insert = ('''
''')

user_table_insert = ('''
''')

song_table_insert = ('''
''')

artist_table_insert = ('''
''')

time_table_insert = ('''
''')

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
