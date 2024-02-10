import psycopg2
import json
import os

from connect import connect
from config import load_config
def get_query_plan(connection, query):
    with connection.cursor() as cursor:
        cursor.execute("EXPLAIN (FORMAT JSON, ANALYZE) " + query)
        query_plan = cursor.fetchone()[0]
        return query_plan

def execute_query(connection, query,boolean=True):
    with connection.cursor() as cursor:
        if boolean:
            cursor.execute(query)
            result = cursor.fetchall()  # Fetch all rows
            return result
        else:
            cursor.execute(query)


def main():
    # Connect to your PostgreSQL database
    config = load_config()
    connection = connect(config)
    search_path = input("Enter search path")
    query_string = f"SET search_path to {search_path}"
    execute_query(connection,query_string,False)
    
    # Sample SQL query
    query = "select * from program join department on program.did=department.did where intake>=40;"

    # # Get query plan
    query_plan = get_query_plan(connection, query)

    existing_data=[]
    if os.path.exists("new.json"):
        with open("new.json",'r') as f:
            existing_data = json.load(f)
    # Print or save the JSON formatted query plan
    existing_data.extend(query_plan)
    # query_result = execute_query(connection, query)
    with open("new.json",'w') as f:
        json.dump(existing_data,f,indent=4)
    # # Print or process the query result
    # for row in query_result:
    #     print(row)  # Print each row

    # Close the database connection
    connection.close()

main()
