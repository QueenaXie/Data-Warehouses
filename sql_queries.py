import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar(500), 
    auth varchar(500), 
    firstname varchar(500), 
    gender varchar(100),
    iteminsession integer, 
    lastname varchar(500), 
    length numeric, 
    level varchar(100), 
    location varchar(500), 
    method varchar(500), 
    page varchar(500),
    registration numeric, 
    session_id integer, 
    song varchar(1000), 
    status integer, 
    ts bigint, 
    useragent varchar(1000), 
    user_id integer
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs integer, 
    artist_id varchar(500),
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar(500),
    artist_name varchar(500),
    song_id varchar(500),
    title varchar(1000),
    duration numeric,
    year int
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
    songplay_id integer identity(0, 1) primary key, 
    start_time timestamp not null,
    user_id integer not null,
    level varchar(100),
    song_id varchar(500),
    artist_id varchar(500),
    session_id integer,
    location varchar(500),
    user_agent varchar(1000)
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
    user_id integer not null primary key,
    first_name varchar(200),
    last_name varchar(200),
    gender varchar(100),
    level varchar(200)
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
    song_id varchar(500) not null primary key,
    title varchar(1000),
    artist_id varchar(500),
    year integer,
    duration numeric
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar(500) not null primary key,
    name varchar(500),
    location varchar(500),
    lattitude numeric,
    longitude numeric
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
    start_time timestamp not null primary key,
    hour integer,
    day integer,
    week integer,
    month varchar(100),
    year integer,
    weekday varchar(100)
);
""")

# STAGING TABLES

staging_events_copy = """
    copy staging_events 
    from {}
    iam_role {}
    compupdate off region 'us-west-2'
    json {}
""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = """
    copy staging_songs 
    from {}
    iam_role {}
    compupdate off region 'us-west-2'
    json 'auto'
""".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
    SELECT distinct events.start_time, events.user_id, events.level, songs.song_id, \
    songs.artist_id, events.session_id, events.location, songs.user_agent FROM \
    (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,\
    * FROM staging_events WHERE page='NextSong') events LEFT JOIN staging_songs songs\
    ON events.song = songs.title AND events.artist = songs.artist_name AND events.length = songs.duration""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level) \
    SELECT distinct user_id, firstname, lastname, gender, level FROM staging_events;
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) \
    SELECT distinct song_id, title, artist_id, year, duration FROM staging_songs where page = 'NextPage';
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude) \
    SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday) \
    SELECT distinct start_time, extract(hr from start_time), extract(d from start_time), extract(w from start_time), \
    extract(mon from start_time), extract(yr from start_time), extract(dayofweek from start_time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
