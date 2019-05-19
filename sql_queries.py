import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = {
    "query": "DROP TABLE IF EXISTS staging_events;",
    "message": "DROPPING staging_events table"
}

staging_songs_table_drop = {
    "query": "DROP TABLE IF EXISTS staging_songs;",
    "message": "DROPPING staging_songs table"
}

songplay_table_drop = {
    "query": "DROP TABLE IF EXISTS fact_songplay;",
    "message": "DROPPING fact_songplay table"
}

user_table_drop = {
    "query": "DROP TABLE IF EXISTS dim_users;",
    "message": "DROPPING dim_users table"
}

song_table_drop = {
    "query": "DROP TABLE IF EXISTS dim_songs;",
    "message": "DROPPING dim_songs table"
}

artist_table_drop = {
    "query": "DROP TABLE IF EXISTS dim_artists;",
    "message": "DROPPING dim_artists table"
}

time_table_drop = {
    "query": "DROP TABLE IF EXISTS dim_time;",
    "message": "DROPPING time table"
}

# CREATE TABLES

staging_events_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS staging_events (
            event_id        INT IDENTITY(0, 1)  NOT NULL    SORTKEY DISTKEY,
            artist          VARCHAR,
            auth            VARCHAR,
            firstname      VARCHAR,
            gender          VARCHAR,
            itemInSession   INTEGER,
            lastname       VARCHAR,
            length          FLOAT,
            level           VARCHAR,
            location        VARCHAR,
            method          VARCHAR(4),
            page            VARCHAR,
            registrtion     BIGINT,
            sessionId       INTEGER,
            song            VARCHAR,
            status          INTEGER,
            ts              TIMESTAMP           NOT NULL,
            userAgent       VARCHAR,
            userId          INTEGER
        );""",
    "message": "CREATING staging_events"
}

staging_songs_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs       INTEGER    NOT NULL    SORTKEY DISTKEY,
            artist_id       VARCHAR    NOT NULL,
            latitude        DECIMAL,
            longitude       DECIMAL,
            location        VARCHAR,
            artist_name     VARCHAR    NOT NULL,
            song_id         VARCHAR    NOT NULL,
            title           VARCHAR    NOT NULL,
            duration        DECIMAL    NOT NULL,
            year            INTEGER    NOT NULL
        );""",
    "message": "CREATING staging_songs"
}

songplay_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS fact_songplay (
            songplay_id     INTEGER     IDENTITY(0,1)   PRIMARY KEY SORTKEY,
            start_time      TIMESTAMP   NOT NULL,
            user_id         INTEGER     NOT NULL,
            level           VARCHAR     NOT NULL,
            song_id         VARCHAR     NOT NULL,
            artist_id       VARCHAR     NOT NULL,
            session_id      INTEGER     NOT NULL,
            location        VARCHAR     NOT NULL,
            user_agent      VARCHAR     NOT NULL
        );""",
    "message": "CREATING fact_songplay"
}

user_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS dim_users (
            user_id         INTEGER     NOT NULL    PRIMARY KEY DISTKEY,
            first_name      VARCHAR     NOT NULL,
            last_name       VARCHAR     NOT NULL,
            gender          CHAR(1)     NOT NULL,
            level           VARCHAR     NOT NULL
        );""",
    "message": "CREATING dim_users"
}

song_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS dim_songs (
            song_id         VARCHAR     NOT NULL    PRIMARY KEY,
            title           VARCHAR     NOT NULL,
            artist_id       VARCHAR     NOT NULL    DISTKEY,
            year            INTEGER     NOT NULL,
            duration        DECIMAL     NOT NULL
        );""",
    "message": "CREATING dim_songs"
}

artist_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS dim_artists (
            artist_id       VARCHAR     NOT NULL    PRIMARY KEY DISTKEY,
            name            VARCHAR     NOT NULL,
            location        VARCHAR     NOT NULL,
            latitude        DECIMAL     NOT NULL,
            longitude       DECIMAL     NOT NULL
        );""",
    "message": "CREATING dim_artists"
}

time_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS dim_time (
            start_time      TIMESTAMP       NOT NULL    PRIMARY KEY SORTKEY DISTKEY,
            hour            INTEGER         NOT NULL,
            day             INTEGER         NOT NULL,
            week            INTEGER         NOT NULL,
            month           INTEGER         NOT NULL,
            year            INTEGER         NOT NULL,
            weekday         INTEGER         NOT NULL
        );""",
    "message": "CREATING dim_time"
}

# STAGING TABLES

staging_events_copy_query = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON {}
    COMPUPDATE OFF REGION 'us-west-2';
    """).format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_events_copy = {
    "query": staging_events_copy_query,
    "message": "COPY staging_events"
}

staging_songs_copy_query = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON 'auto'
    COMPUPDATE OFF REGION 'us-west-2';
    """).format(SONG_DATA, IAM_ROLE)

staging_songs_copy = {
    "query": staging_events_copy_query,
    "message": "COPY staging_songs"
}

# FINAL TABLES

songplay_table_insert = {
    "query": ("""
        INSERT INTO fact_songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                        se.userId AS user_id,
                        se.level AS level,
                        ss.song_id AS song_id,
                        ss.artist_id AS artist_id,
                        se.sessionId AS session_id,
                        se.location AS location,
                        se.userAgent AS user_agent
        FROM staging_events se
        JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;
        """),
    "message": "INSERT fact_songplay"
}

user_table_insert = {
    "query": ("""
        INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
        SELECT DISTINCT userId AS user_id,
                        firstName AS first_name,
                        lastName AS last_name,
                        gender AS gender,
                        level AS level
        FROM staging_events
        WHERE userId IS NOT NULL;
    """),
    "message": "INSERT dim_users"
}

song_table_insert = {
    "query": ("""
        INSERT INTO dim_songs(song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id AS song_id,
                        title AS title,
                        artist_id AS artist_id,
                        year AS year,
                        duration AS duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
    """),
    "message": "INSERT dim_songs"
}

artist_table_insert = {
    "query": ("""
        INSERT INTO dim_artists(artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id AS artist_id,
                        artist_name AS name,
                        location AS location,
                        latitude AS latitude,
                         longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
    """),
    "message": "INSERT dim_artists"
}

time_table_insert = {
    "query": ("""
        INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT ts,
                        EXTRACT(hour FROM ts),
                        EXTRACT(day FROM ts),
                        EXTRACT(week FROM ts),
                        EXTRACT(month FROM ts),
                        EXTRACT(year FROM ts),
                        EXTRACT(weekday FROM ts)
        FROM staging_events
        WHERE ts IS NOT NULL;
    """),
    "message": "INSERT dim_time"
}

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
