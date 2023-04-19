from datetime import datetime
import psycopg2


HOST = "localhost"
PORT = 5432
DB = "tbd_medsos"
TABLE = "socmed"
USER = "medsos"
PASSWORD = "123456"
URL = f"jdbc:postgresql://{HOST}:{PORT}/{DB}"
DRIVER = "org.postgresql.Driver"


def connect():
    conn = None

    try:
        conn = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD)
        cur = conn.cursor()
        cur.execute("SELECT version()")

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def process_row(row, select_cur, insert_cur, update_cur):
    social_media = row[0]
    timestamp = row[1]
    new_unique_count = row[2]
    new_count = row[3]

    sql_string = f"SELECT * FROM {TABLE} WHERE social_media = '{social_media}' AND timestamp = '{timestamp}'"
    select_cur.execute(sql_string)
    records = select_cur.fetchall()

    if len(records) > 0:
        opr = "UPDATE"
        for r in records:
            old_unique_count = r[4]
            old_count = r[3]
    else:
        opr = "INSERT"

    current_timestamp = datetime.now()

    if opr == "UPDATE":
        sql_string = f"""
        UPDATE {TABLE} SET unique_count={old_unique_count + new_unique_count}, count={old_count + new_count}, updated_at='{current_timestamp}' 
        WHERE social_media = '{social_media}' AND timestamp = '{timestamp}'
        """
        update_cur.execute(sql_string)
    elif opr == "INSERT":
        sql_string = f"""
        INSERT INTO {TABLE}(social_media, timestamp, count, unique_count, created_at, updated_at) 
        VALUES ('{social_media}', '{timestamp}', {new_count}, {new_unique_count}, '{current_timestamp}', '{current_timestamp}')
        """
        insert_cur.execute(sql_string)


def process_partition(partition):
    conn = None

    try:
        conn = psycopg2.connect(host=HOST, database=DB, user=USER, password=PASSWORD)
        insert_cur = conn.cursor()
        update_cur = conn.cursor()
        select_cur = conn.cursor()

        for row in partition:
            process_row(row, select_cur, insert_cur, update_cur)

        conn.commit()
        insert_cur.close()
        update_cur.close()
        select_cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    connect()
