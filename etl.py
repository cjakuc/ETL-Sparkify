import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json
from datetime import datetime


def process_song_file(cur, filepath):
    # open song file
    with open(filepath, 'r') as f:
        data = (json.load(f))
    df = pd.DataFrame([data])

    # insert song record
    song_columns = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = df[song_columns]
    for i, row in song_data.iterrows():
        cur.execute(song_table_insert, row.values)
    
    # insert artist record
    artist_columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artist_data = df[artist_columns]
    for i, row in artist_data.iterrows():
        cur.execute(artist_table_insert, row.values)


def process_log_file(cur, filepath):
    # open log file
    logs = []
    for line in open(filepath, 'r'):
        logs.append(json.loads(line))
    df = pd.DataFrame([logs[0]])
    for log in logs[1:]:
        temp_df = pd.DataFrame([log])
        df = df.append(temp_df)

    # filter by NextSong action
    # print(df.head())
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = [datetime.utcfromtimestamp(x/1000) for x in df["ts"]]
    
    # insert time data records
    time_dict = {"start_time": [],
             "hour": [],
             "day": [],
             "week": [],
             "month": [],
             "year": [],
             "weekday": []}
    time_dict["start_time"] = [x.strftime('%Y-%m-%dT%H:%M:%S.%f') for x in t]
    # Add hour
    time_dict["hour"] = [x.hour for x in t]
    # Add day
    time_dict["day"] = [x.day for x in t]
    # Add week
    time_dict["week"] = [x.isocalendar()[1] for x in t]
    # Add month
    time_dict["month"] = [x.month for x in t]
    # Add year
    time_dict["year"] = [x.year for x in t]
    # Add weekday
    time_dict["weekday"] = [x.weekday() for x in t]
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_columns = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = df[user_columns]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        time_stamp = datetime.utcfromtimestamp(row.ts/1000).strftime('%Y-%m-%dT%H:%M:%S.%f')
        userid = row.userId
        level = row.level
        sessionid = row.sessionId
        location = row.location
        useragent = row.userAgent
        songplay_data = (time_stamp, userid, level, songid, artistid, sessionid, location, useragent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 port=5433 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()