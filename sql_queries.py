# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL
    ,start_time TIMESTAMP NOT NULL REFERENCES time(start_time)
    ,user_id SERIAL NOT NULL REFERENCES users(user_id)
    ,level VARCHAR
    ,song_id SERIAL REFERENCES songs(song_id)
    ,artist_id SERIAL REFERENCES artists(artist_id)
    ,session_id SERIAL
    ,location VARCHAR
    ,user_agent VARCHAR
    ,PRIMARY KEY (songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL
    ,first_name VARCHAR
    ,last_name VARCHAR
    ,gender VARCHAR
    ,level VARCHAR
    ,PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id SERIAL
    ,title VARCHAR
    ,artist_id SERIAL
    ,year INT
    ,duration INT
    ,PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id SERIAL
    ,name VARCHAR
    ,location VARCHAR
    ,latitude DOUBLE PRECISION
    ,longitude DOUBLE PRECISION
    ,PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP
    ,hour INT
    ,day INT
    ,week INT
    ,month INT
    ,year INT
    ,weekday INT
    ,PRIMARY KEY (start_time)
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
    songplay_id
    ,start_time
    ,user_id
    ,level
    ,song_id
    ,artist_id
    ,session_id
    ,location
    ,user_agent
) VALUES (
    {},{},{},{},{},{},{},{},{}
);
""")

user_table_insert = ("""
INSERT INTO users (
    user_id
    ,first_name
    ,last_name
    ,gender
    ,level
) VALUES (
    {},{},{},{},{}
) ON CONFLICT(user_id) 
    DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id
    ,title
    ,artist_id
    ,year
    ,duration
) VALUES (
    {},{},{},{},{}
) ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id
    ,name
    ,location
    ,latitude
    ,longitude
) VALUES (
    {},{},{},{},{}
) ON CONFLICT (artists) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (
    start_time
    ,hour
    ,day
    ,week
    ,month
    ,year
    ,weekday
) VALUES (
    {},{},{},{},{},{},{}
) ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT 
    songs.song_id AS song_id
    ,songs.artist_id AS artist_id
FROM
    songs
    JOIN artists
        ON artists.artists_id = songs.artist_id
WHERE
    songs.title = {}
    AND
    artists.name = {}
    AND
    songs.duration = {}
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]