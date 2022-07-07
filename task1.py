import argparse
from db_connector import db_factory


def tasks_1(db_type):
    """
    Query visitor created the most revenue
    :param conn: the Connection object
    :return:
    """

    sql ="""SELECT visitor_id, SUM(revenue) AS sum_revenues 
                    FROM Transactions  
                    GROUP BY visitor_id 
                    ORDER BY sum_revenues 
                    DESC LIMIT 1"""

    db_handle = db_factory(db_type)
    result = db_handle.run_select_query(sql)
    for row in result:
        print( f"Visitor ID: {row[0]}. Total Revenue {row[1]}.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default="sqlite")
    args = parser.parse_args()
    tasks_1(args.db)

