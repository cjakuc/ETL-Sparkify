import os
import glob
import psycopg2
import psycopg2.extras as extras
import pandas as pd
from sql_queries import *
import json
from datetime import datetime

def execute_values(cur, query, df, ts=False):
    """
    Uses psycopg2.extras.execute_values() to bulk insert

    Parameters
    ----------
    cur : psycopg2.connect.cursor()
        cursor to execute the queries
    query : string
        SQL query to execute
    df : pandas.DataFrame
        DataFrame containing the data to be inserted into the database
    """
    # List of tuples from the df values
    tuples = [tuple(x) for x in df.to_numpy()]
    
    try:
        extras.execute_values(cur, query, tuples)
    except psycopg2.Error as e:
        print(f"Error: {e}")
        return 1

def create_time_dict(timestamps):
    """
    Take the timestamps in milliseconds and convert to
    appropriate types for the time table

    Parameters
    ----------
    timestamps : pandas Series
        column of dataframe with millisecond timestamps as integers

    Returns
    -------
    time_dict
        python dictionary ready to be turned into a dataframe
        and inserted into the time table
    """
    time_dict = {}
    # convert timestamp column to datetime
    t = [datetime.utcfromtimestamp(x/1000) for x in timestamps]
    
    # insert time data records
    time_dict = {}
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
    return time_dict


def process_song_file(cur, filepath):
    """
    Parse a song file and insert the correct data into the songs
    and artists tables

    Parameters
    ----------
    cur : psycopg2.connect.cursor()
        cursor to execute the queries
    filepath : string
        path to file
    """
    # open song file
    with open(filepath, 'r') as f:
        data = (json.load(f))
    df = pd.DataFrame([data])

    # insert song record
    song_columns = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = df[song_columns]
    execute_values(cur=cur, query=song_table_insert, df=song_data)
    
    # insert artist record
    artist_columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artist_data = df[artist_columns]
    execute_values(cur=cur, query=artist_table_insert, df=artist_data)


def process_log_file(cur, filepath):
    """
    Parse a log file and insert the correct data into the time,
    users, and songplays tables

    Parameters
    ----------
    cur : psycopg2.connect.cursor()
        cursor to execute the queries
    filepath : string
        path to file
    """
    df = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # create time_dict and turn it into a dataframe
    time_dict = create_time_dict(df["ts"])
    time_df = pd.DataFrame.from_dict(time_dict)

    # execute_values implementation
    execute_values(cur=cur, query=time_table_insert, df=time_df)

    # load user table
    user_columns = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = df[user_columns]

    # drop the duplicate rows so the ON CONFLICT UPDATE of level works
    # because execute_values is running all queries at once so we can't update
    # the same row twice.
    # solution is to base duplicates off every col but level and keep the last duplicate
    # row with the most recent level value
    final_user_df = user_df.drop_duplicates(subset=["userId", "firstName", "lastName", "gender"],keep='last',ignore_index=True)

    # insert user records
    execute_values(cur=cur, query=user_table_insert, df=final_user_df)

    # insert songplay records
    # single row insert implementation
    songId = []
    artistId = []
    song_select_cache = {}
    for index, row in df.iterrows():
        
        if f"{row.song}{row.artist}{row.length}" in song_select_cache:
            songid, artistid = song_select_cache[f"{row.song}{row.artist}{row.length}"]
        else:
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
            
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None
            
            song_select_cache[f"{row.song}{row.artist}{row.length}"] = (songid, artistid)

        songId.append(songid)
        artistId.append(artistid)

    
    songplays_df = df
    songplays_df['songId'] = pd.Series(songId)
    songplays_df['artistId'] = pd.Series(artistId)
    songplays_df['ts'] = pd.Series(time_dict['start_time'], index=songplays_df.index)
    
    # Add song_id and artist_id columns
    
    songplays_columns = ['ts', 'userId', 'level', 'songId', 'artistId', 'sessionId', 'location', 'userAgent']
    songplays_df = songplays_df[songplays_columns]
    execute_values(cur=cur, query=songplay_table_insert, df=songplays_df)


def process_data(cur, conn, filepath, func):
    """
    Iterate through the files, process the data within,
    and insert it into the correct tables

    Parameters
    ----------
    cur : psycopg2.connect.cursor()
        cursor to execute the queries
    conn : psycopg2.connect
        connection to the database
    filepath : string
        path to files
    func : function
        appropriate function to process the correct type of file
    """
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
        # conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 port=5433 dbname=sparkifydb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()