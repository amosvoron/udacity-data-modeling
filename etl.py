import os
import sys
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Extracts songs and artists from the JSON data source
    and inserts them into the database.
    
    Args:
        cur: The PostgreSQL cursor object. 
        filepath (str): The filepath to the JSON data source.
        
    Returns:
        None
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    cur.execute(song_table_insert, tuple(song_data.values[0]))
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    cur.execute(artist_table_insert, tuple(artist_data.values[0]))

    
def process_log_file(cur, filepath):
    """Extracts time data, users and songplay data from the JSON data source
    and inserts them into the database.
    
    Args:
        cur: The PostgreSQL cursor object. 
        filepath (str): The filepath to the JSON data source.
        
    Returns:
        None
    """    
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
    #
    # ATTENTION:
    # We extract user data by using 5 keys (userId, firstName, lastName, gender, level).
    # The first four keys guarantee the uniqueness of the userId which is a primary key.
    # Since the level key is not user-related but user-time-related attribute there will still
    # remain some duplicates in cases when user changes the subscription level. These duplicates
    # will be handled by ON CONFLICT DO UPDATE clause providing the last level in sequence.
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
    """Processes all data through the ETL pipeline.
    
    Args:
        cur: The PostgreSQL cursor object. 
        conn: The PostgreSQL connection object.
        filepath (str): The filepath to the JSON data source.
        func: The ETL data source specific function to call.
        
    Returns:
        None
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
        conn.commit()
        print(' > {}/{} files processed.'.format(i, num_files))

        
def database_check(cur):
    """Performs database check operations like rowcount checks
    and database referential integrity check.
    
    If the database passes the referential integrity check,
    the foreign keys are created.
    
    Args:
        cur: The PostgreSQL cursor object. 
    
    Returns:
        None
    """        
    
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
    matched_rows = cur.fetchone()[0]
    print(f'Matched rows: {matched_rows} of {total_rows}')
    
    # Create foreign keys if the database passes the referential integrity check
    if matched_rows == total_rows:
        cur.execute(create_foreign_keys)
        print('Foreign keys created.')
    else:
        print(f'Database did not pass the referential integrity check.')
    print()
    

def main():
    
    # Check input paramater - the root directory
    if len(sys.argv) < 2:
        print('Missing parameter: Please provide a data root directory parameter.' +
              '\nPay attention that the song and log data subdirectories must be located inside the root directory:' +
              '\n  /song_data'
              '\n  /log_data')
        return
    
    data_dir = sys.argv[1]
    if os.path.isdir(data_dir) == False:
        print(f'Invalid parameter: Data directory "{data_dir}" does not exist.\n')
        return
        
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
     
    print('Song data processing started...')
    process_data(cur, conn, filepath=os.path.join(data_dir, 'song_data'), func=process_song_file)
    print('Song data processing completed.')
    
    print('Log data processing started...')
    process_data(cur, conn, filepath=os.path.join(data_dir, 'log_data'), func=process_log_file)
    print('Log data processing completed.')
    
    # check database
    database_check(cur)

    conn.close()


if __name__ == "__main__":
    main()