import sqlite3
import pandas as pd


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Exception as e:
        print(e)

    return conn


def tasks_3(conn):
    """
    Query combines the contents of 
    Devices and Transactions tables.
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("""SELECT Transactions.id, 
                        Transactions.datetime, 
                        Transactions.visitor_id, 
                        Transactions.device_type, 
                        Transactions.revenue,
                        Transactions.tax, Devices.device_name  
                        FROM Transactions 
                        INNER JOIN Devices 
                        ON  Devices.id = Transactions.device_type """)
    rows = cur.fetchall()
    names = list(map(lambda x: x[0], cur.description))
    pd.DataFrame(rows, columns=names).to_csv("combined_table.csv")
    conn.close()

if __name__ == "__main__":
    conn = create_connection("transactions.db")
    tasks_3(conn)