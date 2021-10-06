import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, conn, files_list):
    """
    - Reads the song files
    - Creates a pandas DataFrame
    - Executes queries imported from sql_queries modules
    """
    
    # open song file
    # df = pd.read_json(filepath, lines=True)
    
    for i in range(len(files_list)):
        if i == 0:
            df = pd.read_json(files_list[i], lines=True)
        else:
            to_append_df = pd.read_json(files_list[i], lines=True)
            df = df.append(to_append_df, ignore_index=True)        
        
    
    
    # insert song record
    song_data = df[['song_id', 'artist_id', 'title', 'year', 'duration']]
    # cur.execute(song_table_insert, song_data)
    
    for index, row in song_data.iterrows():
        cur.execute(song_table_insert, row)
        conn.commit()
    
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    # cur.execute(artist_table_insert, artist_data)
    
    for index, row in artist_data.iterrows():
        cur.execute(artist_table_insert, row)
        conn.commit()


def process_log_file(cur, conn, files_list):
    """
    - Reads the log files
    - Creates a pandas DataFrame
    - Executes queries imported from sql_queries modules
    """
    
    # open log file
    # df = pd.read_json(filepath, lines=True)
    
    for i in range(len(files_list)):
        if i == 0:
            df = pd.read_json(files_list[i], lines=True)
        else:
            to_append_df = pd.read_json(files_list[i], lines=True)
            df = df.append(to_append_df, ignore_index=True)
        
    
    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    t2 = t.dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # insert time data records
    time_data = [t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame({'start_time': t2.values, 'hour': t.dt.hour, 'day': t.dt.day, 'week_of_year': t.dt.weekofyear, 'month': t.dt.month, 'year': t.dt.year, 'weekday': t.dt.day_name()})

    for index, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for index, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        conn.commit()

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
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()


def process_data(cur, conn, filepath, func):
    """
    - Walk through the directories from the root
    - take the files names and append them into a list
    - Call a method to read and process the files 
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
    func(cur, conn, all_files)
#     for i, datafile in enumerate(all_files, 1):
#         func(cur, datafile)
#         conn.commit()
#         print('{}/{} files processed.'.format(i, num_files))
    

def main():
    """
    - Connects to the sparkifydb
    - Creates the cursor to sparkfydb
    - Call the methods in order to process and insert data to tables in sparkfydb
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()