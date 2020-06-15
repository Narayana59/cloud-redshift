import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
    This procedure copies events and log files which reside in Amazon S3 buckets 
    and load them into respective stage tables in Redshift.

    INPUTS: 
    * cur the cursor variable
    * conn variabele to connect to the date and commit the transactions
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
"""
    This procedure processes and transfors the data from stage tables in Redshift
    and insert data into respective fact and dimension postgres tables.

    INPUTS: 
    * cur the cursor variable
    * conn variabele to connect to the date and commit the transactions
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


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