# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id SERIAL PRIMARY KEY, 
            start_time TIMESTAMP NOT NULL, 
            user_id INT NOT NULL, 
            level VARCHAR, 
            song_id VARCHAR,
            artist_id VARCHAR, 
            session_id INT, 
            location VARCHAR, 
            user_agent VARCHAR
            );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR, 
        artist_id VARCHAR, 
        title VARCHAR, 
        year INT, 
        duration FLOAT, 
        PRIMARY KEY (song_id)
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY, 
        artist_name VARCHAR, 
        artist_location VARCHAR, 
        artist_latitude FLOAT, 
        artist_longitude FLOAT
        );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour INT, 
        day INT, 
        week_of_year INT, 
        month INT, 
        year INT, 
        weekday VARCHAR
        );
""")

# INSERT RECORDS
# timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent
songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, user_agent)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, 
        artist_id, 
        title, 
        year, 
        duration) 
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude) 
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
    INSERT INTO time (
        start_time, 
        hour, 
        day, 
        week_of_year, 
        month, 
        year, 
        weekday)
    VALUES(%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""
    SELECT 
        s.song_id, 
        s.artist_id 
        FROM songs s
        INNER JOIN artists a
        ON s.artist_id = a.artist_id
        WHERE s.title = (%s) AND a.artist_name = (%s) AND s.duration = (%s);
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]