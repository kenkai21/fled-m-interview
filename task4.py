import xml.etree.ElementTree as ET
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



def parse_xml(path):
    """ Parse xml file and get the 
    exchange rates for Euros in their 
    corresponding date
    :param path: File path
    :return: Dictionary
    """
    dictt = {}

    root = ET.parse(path)
    all_day_nodes = root.findall('./')[2].tag
    print(all_day_nodes)
    c = root.findall(all_day_nodes)

    #looping through xml elements to get date and USD values
    for elem in c[0].getiterator():
        day = elem.attrib.get('time')
        if day is not None:
            for ee in elem.getiterator():
                if ee.attrib.get('currency') == 'USD':
                    dictt[day] = float(ee.attrib.get('rate'))

    return {k: v for k, v in dictt.items() if v is not None}


def tasks_4(conn, dictt):
    """
    Query combines the contents of
    Devices and Transactions tables.
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
  
    for k, v in dictt.items():
        sql = f"UPDATE Transactions set revenue = {v} * revenue WHERE cast(date(datetime) as varchar) = cast('{k.strip()}' as varchar)"
        cur.execute(sql)
        print(f'{cur.rowcount} rows affected for day {k}')
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    conn = create_connection("transactions.db")
    dictt = parse_xml('eurofxref-hist-90d.xml')
    tasks_4(conn, dictt)



