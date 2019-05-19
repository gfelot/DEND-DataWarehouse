import sys
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load the data from the S3 bucket to the staging table in Redshift
    :param cur: cursor connexion object on redshift
    :param conn: connection object on the redshift
    """
    for o in copy_table_queries:
        print(o['message'])
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()
            sys.exit(1)


def insert_tables(cur, conn):
    """
    Insert data from the staging table to the dimension and fact table
    :param cur: cursor connexion object on redshift
    :param conn: connection object on the redshift
    """
    for o in insert_table_queries:
        print(o['message'])
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()
            sys.exit(1)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
