# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time timestamp NOT NULL UNIQUE,
        user_id int NOT NULL,
        level char(4) NOT NULL,
        song_id varchar(20) NULL,
        artist_id varchar(20) NULL,
        session_id int NOT NULL,
        location varchar NOT NULL,
        user_agent varchar NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int NOT NULL PRIMARY KEY,
        first_name varchar(50) NOT NULL,
        last_name varchar(50) NOT NULL,
        gender char(1) NOT NULL,
        level char(4) NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar(20) NOT NULL PRIMARY KEY,
        title varchar(100) NOT NULL,
        artist_id varchar(20) NOT NULL,
        year int NOT NULL,
        duration numeric NOT NULL
    );
""")

# artist_id, name, location, latitude, longitude

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar(20) NOT NULL PRIMARY KEY,
        name varchar(100) NOT NULL,
        location varchar(50) NOT NULL,
        latitude varchar(30) NULL,
        longitude varchar(30) NULL
    );
""")

# start_time, hour, day, week, month, year, weekday

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL PRIMARY KEY,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday int NOT NULL
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) 
        DO NOTHING;
""")

# Note that ON CONFLICT we perform DO UPDATE
# which will provide the last level in sequence
user_table_insert = ("""
    INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) 
        DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO songs
    (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) 
        DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) 
        DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) 
        DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT A.song_id, A.artist_id 
    FROM songs AS A
    INNER JOIN artists AS B ON A.artist_id = B.artist_id
    WHERE A.title = %s
        AND B.name = %s
        AND A.duration = %s;
""")

# DATABASE CHECK

tables_check = ("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema='public';
""")

table_rowcount_check = ("""
    SELECT 
        (SELECT COUNT(*) FROM songplays) AS songplays_count
        , (SELECT COUNT(*) FROM time) AS time_count    
        , (SELECT COUNT(*) FROM users) AS users_count        
        , (SELECT COUNT(*) FROM songs) AS songs_count
        , (SELECT COUNT(*) FROM artists) AS artists_count;     
""")

# ATTENTION
#   With full dataset the left outer joins can be replaced by inner joins!
#   In our case of partial dataset the songs and artists only matches 1 row.
database_integrity_check = ("""
    SELECT COUNT(*) 
    FROM songplays AS A
    INNER JOIN users AS B ON A.user_id = B.user_ID
    INNER JOIN time AS C ON A.start_time = C.start_time
    LEFT OUTER JOIN songs AS D ON A.song_id = D.song_id
    LEFT OUTER JOIN artists AS E ON A.artist_id = E.artist_id;
""")

create_foreign_keys = ("""
    ALTER TABLE songs
    ADD CONSTRAINT fk_songs_artists FOREIGN KEY(artist_id)
    REFERENCES artists(artist_id);
    
    ALTER TABLE songplays
    ADD CONSTRAINT fk_songplays_users FOREIGN KEY(user_id)
    REFERENCES users(user_id);
    
    ALTER TABLE songplays
    ADD CONSTRAINT fk_songplays_songs FOREIGN KEY(song_id)
    REFERENCES songs(song_id);

    ALTER TABLE songplays
    ADD CONSTRAINT FK_songplays_artists FOREIGN KEY(artist_id)
    REFERENCES artists(artist_id); 
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]