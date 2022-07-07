import sqlite3

import psycopg2


class SqliteConnector:

    
    def open_conn(self):
        self.conn = self.create_connection("transactions.db")
        

    def close_conn(self):
        self.conn.close()

    def create_connection(self, db_file):
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


    def run_select_query(self, sql):
        self.open_conn()
        cur = self.conn.cursor()
        cur.execute(sql)
        results = cur.fetchall()
        self.close_conn()
        return results

    def run_update_query(self, sql):
        self.open_conn()
        cur = self.conn.cursor()
        cur.execute(sql)
        affected_rows = cur.rowcount()
        self.conn.commit()
        self.close_conn()
        return affected_rows





class PostgreSqlConnector:

    def __init__(self):
        self.conn = self.create_connection()
        
        

    def close_conn(self):
        self.conn.close()


    def create_connection(self, database="task", user="postgres", password="password", host="127.0.0.1", port="5432"):
        """ create a database connection to the postgres database
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        except Exception as e:
            print(e)

        return conn
        


    def run_select_query(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        results = cur.fetchall()
        self.close_conn()
        return results

    def run_update_query(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        affected_rows = cur.rowcount()
        self.conn.commit()
        self.close_conn()
        return affected_rows



def db_factory(db_type):
    assert db_type in ['sqlite', 'postgres']

    if db_type == "sqlite":
        return SqliteConnector()
    else: 
        return PostgreSqlConnector()
