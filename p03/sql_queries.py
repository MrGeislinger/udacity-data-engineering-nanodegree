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
    COPY staging_events 
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2' 
    COMPUPDATE OFF STATUPDATE OFF
    FORMAT as JSON '{}'
''').format(
        config.get('S3','LOG_DATA'),
        config.get('IAM_ROLE','ARN'),
        config.get('S3','LOG_JSONPATH')
)

staging_songs_copy = ('''
    COPY staging_songs 
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2' 
    COMPUPDATE OFF STATUPDATE OFF
    FORMAT as JSON 'auto'
''').format(
        config.get('S3','SONG_DATA'),
        config.get('IAM_ROLE','ARN'),
)

# FINAL TABLES

# Join if event song & artist match; only next songs
songplay_table_insert = ('''
INSERT INTO (
    start_time
    ,user_id
    ,level
    ,song_id
    ,artist_id
    ,session_id
    ,location
    ,user_agent
)
SELECT
    TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 second' AS start_time, 
    ,events.user_id AS user_id
    ,events.level AS level
    ,events.song_id AS song_id
    ,songs.artist_id AS artist_id
    ,events.session_id AS session_id
    ,songs.artist_location AS location
    ,events.user_agent AS user_agent
FROM
    staging_events AS events
    JOIN staging_songs AS songs
        ON songs.song_id = events.song_id
            AND songs.artist_name = events.artist
WHERE
    events.page = 'NextSong'
''')

user_table_insert = ('''
INSERT INTO (
    user_id
    ,first_name
    ,last_name
    ,gender
    ,level
)
SELECT DISTINCT
    events.user_id AS user_id
    ,events.first_name AS first_name
    ,events.last_name AS last_name
    ,events.gender AS gender
    ,events.level AS level
FROM
    staging_events AS events
WHERE
    events.user_id IS NOT NULL
''')

song_table_insert = ('''
INSERT INTO (
    song_id
    ,title
    ,artist_id
    ,year
    ,duration
)
SELECT DISTINCT
    songs.song_id AS song_id
    ,songs.title AS title
    ,songs.artist_id AS artist_id
    ,songs.year AS year
    ,songs.duration AS duration
FROM
    staging_songs AS songs
WHERE
    songs.song_id IS NOT NULL
''')

artist_table_insert = ('''
INSERT INTO (
    artist_id
    ,name
    ,location
    ,latitude
    ,longitude
)
SELECT DISTINCT
    songs.artist_id AS artist_id
    ,events.artist_name AS name
    ,songs.artist_location AS location
    ,songs.artist_latitude AS latitude
    ,songs.artist_longitude AS longitude
FROM
    staging_songs AS songs
WHERE
    songs.artist_id IS NOT NULL
''')

time_table_insert = ('''
INSERT INTO (
    start_time
    ,hour
    ,day
    ,week
    ,month
    ,year
    ,weekday
)
SELECT
    start_time AS start_time
    ,EXTRACT(hr FROM start_time) AS hour
    ,EXTRACT(d FROM start_time) AS day
    ,EXTRACT(w FROM start_time) AS week
    ,EXTRACT(mon FROM start_time) AS month
    ,EXTRACT(yr FROM start_time) AS year
    ,EXTRACT(weekday FROM start_time) AS weekday
FROM (
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 second' AS start_time 
    FROM 
        staging_events AS events
)
''')

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
