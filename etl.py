import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    cur.execute(song_table_insert, tuple(song_data.values[0]))
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    cur.execute(artist_table_insert, tuple(artist_data.values[0]))

    
def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    # Note: we use isocalendar function because Series.dt.week has been deprecated    
    time_df = pd.DataFrame({
        'start_time': t,
        'hour': t.dt.hour,
        'day': t.dt.day,
        'week': t.apply(lambda x: x.isocalendar()[1]),
        'month': t.dt.month,
        'year': t.dt.year,
        'weekday': t.dt.weekday
    })
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, tuple(row))  

    # load user table
    # Note: We extract all distict users
    # Remark on the 'level' column:
    #   
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()
        
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, tuple(row))
        
    # add start_time to dataframe
    df['start_time'] = t
        
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
        songplay_data = (row.start_time, \
                         row.userId, \
                         row.level, \
                         songid, \
                         artistid, \
                         row.sessionId, \
                         row.location, \
                         row.userAgent)
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
        print(' > {}/{} files processed.'.format(i, num_files))

        
def database_check(cur):
    
    print('\n-----------------------------------------')
    print('  Table rowcount check')
    print('-----------------------------------------')
    tables = ['songplays', 'time', 'users', 'songs', 'artists']
    total_rows = 0
    cur.execute(table_rowcount_check)
    for count in cur.fetchall()[0]:
        print(f'{tables.pop(0)}: {count}')  
        if total_rows == 0:
            total_rows = count
    
    print('\n-----------------------------------------')
    print('  Database referential integrity check ')
    print('-----------------------------------------')
    cur.execute(database_integrity_check)
    print(f'Matched rows: {cur.fetchone()[0]} of {total_rows}')
    print()
    

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    data_dir = 'D:/udacity/Data Engineering/project 1/data/'
    
    print('Song data processing started...')
    process_data(cur, conn, filepath=data_dir+'song_data', func=process_song_file)
    print('Song data processing completed.')
    
    print('Log data processing started...')
    process_data(cur, conn, filepath=data_dir+'log_data', func=process_log_file)
    print('Log data processing completed.')
    
    # check database
    database_check(cur)

    conn.close()


if __name__ == "__main__":
    main()