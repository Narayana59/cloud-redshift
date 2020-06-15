import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP table staging_events"
staging_songs_table_drop = "DROP table staging_songs"
songplay_table_drop = "DROP table songplays"
user_table_drop = "DROP table users"
song_table_drop = "DROP table songs"
artist_table_drop = "DROP table artists"
time_table_drop = "DROP table time"

# CREATE TABLES

staging_events_table_create= ("CREATE TABLE IF NOT EXISTS staging_events (artist varchar,auth varchar,firstname varchar ,gender varchar,iteminsession varchar,lastname varchar, length float,level varchar, location varchar,method varchar, page varchar,registration varchar,sessionid varchar,song varchar,status varchar, ts varchar, useragent varchar,userId varchar);")

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs  (artist_id varchar, artist_latitude varchar , artist_location varchar,artist_longitude varchar,artist_name varchar,duration varchar,num_songs int,song_id varchar ,title varchar ,year varchar) ")

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays  (songplay_id  int IDENTITY(0,1)  PRIMARY KEY  , start_time timestamp NOT NULL, user_id int NOT NULL, level varchar NOT NULL, song_id varchar NOT NULL, artist_id varchar NOT NULL, session_id int NOT NULL, location varchar NOT NULL, user_agent varchar NOT NULL );")

user_table_create = ("CREATE TABLE IF NOT EXISTS users    (user_id int PRIMARY KEY, first_name varchar , last_name varchar, gender varchar, level varchar);")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs   (song_id varchar PRIMARY KEY, title varchar NOT NULL, artist_id varchar, year int, duration int);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists   (artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar, latitude int , longitude int  );")

time_table_create = ("CREATE TABLE IF NOT EXISTS time   (start_time timestamp PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int);")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data/'
    credentials 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json' compupdate off region 'us-west-2';
""").format(*config['IAM_ROLE'].values())

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    format as json 'auto' compupdate off region 'us-west-2';
""").format(*config['IAM_ROLE'].values())

# FINAL TABLES

songplay_table_insert = ("insert into songplays  (start_time , user_id , level , song_id , artist_id , session_id , location , user_agent  ) SELECT TIMESTAMP 'epoch' + events.ts/1000 *INTERVAL '1 second' as start_time,events.userId::integer,events.level,         songs.song_id,songs.artist_id,events.sessionid::integer,events.location,events.useragent FROM staging_events AS events JOIN staging_songs AS songs  ON (events.artist = songs.artist_name) AND (events.song = songs.title) AND (events.length = songs.duration) WHERE events.page = 'NextSong'")

user_table_insert = ("insert into users(user_id, first_name , last_name , gender , level ) select distinct userid::integer , firstname  , lastname , gender , level from staging_events where userid is not null and userid <> ' ' and page = 'NextSong' ")

song_table_insert = ("insert into songs (song_id,title,artist_id,year,duration) select distinct song_id,title,artist_id,year::integer,duration::float from staging_songs ")

artist_table_insert = ("insert into artists   (artist_id , name , location , latitude , longitude ) select distinct artist_id , artist_name , artist_location , artist_latitude::float , artist_longitude::float from staging_songs ")

time_table_insert = ("insert into time   (start_time , hour , day , week , month , year , weekday ) SELECT a.start_time, EXTRACT (HOUR FROM a.start_time), EXTRACT (DAY FROM a.start_time),EXTRACT (WEEK FROM a.start_time), EXTRACT (MONTH FROM a.start_time),EXTRACT (YEAR FROM a.start_time), EXTRACT (WEEKDAY FROM a.start_time) FROM (SELECT distinct TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_events where page = 'NextSong') a ")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy,staging_events_copy ]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
