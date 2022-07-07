import argparse
from db_connector import db_factory


def tasks_2(db_type):
    """
    Query day most revenue for users who
    ordered via a mobile phone was created
    :param conn: the Connection object
    :return:
    """

    sql = """SELECT * FROM (
                        SELECT date(datetime), sum(revenue) AS total  
                        FROM Transactions  
                        WHERE device_type=3  
                        GROUP BY date(datetime)) AS t 
                        ORDER BY t.total 
                        DESC LIMIT 1"""


    db_handle = db_factory(db_type)
    result = db_handle.run_select_query(sql)

    for row in result:
        print( f"The maximum revenue for mobile phone users is {row[1]}, it was was ordered on {row[0]}.")
   



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default="sqlite")
    args = parser.parse_args()
    tasks_2(args.db)

