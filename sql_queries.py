# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(songplay_id SERIAL PRIMARY KEY,
                                     start_time timestamp NOT NULL REFERENCES time ON DELETE SET NULL,
                                     user_id int NOT NULL REFERENCES users ON DELETE SET NULL,
                                     level varchar,
                                     song_id varchar REFERENCES songs ON DELETE SET NULL,
                                     artist_id varchar REFERENCES artists ON DELETE SET NULL,
                                     session_id int,
                                     location varchar,
                                     user_agent varchar)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(user_id int PRIMARY KEY,
                                 first_name varchar,
                                 last_name varchar,
                                 gender varchar,
                                 level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY,
                                 title varchar,
                                 artist_id varchar,
                                 year int,
                                 duration real)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY,
                                   name varchar,
                                   location varchar,
                                   latitude real,
                                   longitude real)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(start_time timestamp PRIMARY KEY,
                                hour double precision,
                                day double precision,
                                week double precision,
                                month double precision,
                                year double precision,
                                weekday double precision)
""")

# ADD FOREIGN KEYS

# songplay_table_fks = """
# ALTER TABLE songplays
# ADD CONSTRAINT fk_start_time
#         FOREIGN KEY (start_time)
#             REFERENCES time (start_time)
#             ON DELETE SET NULL,
# ADD CONSTRAINT fk_user_id
#         FOREIGN KEY (user_id)
#             REFERENCES users (user_id)
#             ON DELETE SET NULL,
# ADD CONSTRAINT fk_song_id
#         FOREIGN KEY (song_id)
#             REFERENCES songs (song_id)
#             ON DELETE NO ACTION,
# ADD CONSTRAINT fk_artist_id
#         FOREIGN KEY (artist_id)
#             REFERENCES artists (artist_id)
#             ON DELETE NO ACTION;
# """

# user_table_fks = """
# ALTER TABLE users
# ADD CONSTRAINT
#     FOREIGN KEY (fk_columns)
#         REFERENCES parent_table (parent_key_columns)
#         ON DELETE SET NULL;
# """

# song_table_fks = """
# ALTER TABLE songs
# ADD CONSTRAINT fk_artist_id
#     FOREIGN KEY (artist_id)
#         REFERENCES artists (artist_id)
#         ON DELETE SET NULL;
# """

# artist_table_fks = """
# ALTER TABLE artists
# ADD CONSTRAINT
#     FOREIGN KEY (fk_columns)
#         REFERENCES parent_table (parent_key_columns)
#         ON DELETE SET NULL;
# """

# time_table_fks = """
# ALTER TABLE time
# ADD CONSTRAINT
#     FOREIGN KEY (fk_columns)
#         REFERENCES parent_table (parent_key_columns)
#         ON DELETE SET NULL;
# """

# INSERT RECORDS

songplay_table_insert = (
"""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES %s
"""
)

user_table_insert = (
"""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES %s
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
"""
)

song_table_insert = (
"""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES %s
"""
)

artist_table_insert = (
"""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES %s
ON CONFLICT (artist_id) DO NOTHING
"""
)


time_table_insert = (
"""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES %s
ON CONFLICT (start_time) DO NOTHING
"""
)

# FIND SONGS

song_select = (
"""
SELECT
    songs.song_id,
    songs.artist_id
FROM
    songs
INNER JOIN artists ON artists.artist_id = songs.artist_id
WHERE
    songs.title = %s
    AND artists.artist_id = %s
    AND songs.duration = %s
"""
)

# QUERY LISTS

create_table_queries = [user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create,
                        songplay_table_create
                        # ,songplay_table_fks 
                        # ,song_table_fks
                        ]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]