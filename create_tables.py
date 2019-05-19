import sys
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all the table present in the Redshift cluster
    :param cur: cursor connexion object on Redshift
    :param conn: connection object on Redshift
    """
    for o in drop_table_queries:
        print(o['message'])
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()
            sys.exit(1)


def create_tables(cur, conn):
    """
    Create table in the Redshift cluster
    :param cur: cursor connexion object on Redshift
    :param conn: connection object on Redshift
    """
    for o in create_table_queries:
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

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except Exception as e:
        print(e)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
