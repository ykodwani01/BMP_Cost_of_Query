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
    print("Hello")
    config = load_config()
    connection = connect(config)
    
    # search_path = input("Enter search path: ")
    # query_string = f"SET search_path to {search_path}"
    # execute_query(connection,query_string,False)
    
    # Sample SQL query
    query = '''select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '1 day'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
LIMIT 1;


    '''

    # n= int(input("Number of databases used: "))
    # databases = []
    # # for i in range(n):
    #     db = input("Name of databases:")
    #     databases.append(db)
    
    # avg_row_length = []
    # for databases_loop in databases:
    #     query2 = f'''SELECT json_agg(json_build_object(
    #     'rowsize_in_bytes', rowsize_in_bytes
    #     )) AS json_output
    #     FROM (
    #     SELECT avg(pg_column_size({databases_loop}.*)) as rowsize_in_bytes
    #     FROM {databases_loop}
    #     ) AS subquery;'''
    #     avg_row_length.append(execute_query(connection,query2,True)[0][0][0]['rowsize_in_bytes'])

    # no_of_partitions = []

    # for databases_loop in databases:
    #     query3 = f'''SELECT
    #         nmsp_parent.nspname AS parent_schema,
    #         parent.relname      AS parent,
    #         nmsp_child.nspname  AS child_schema,
    #         child.relname       AS child
    #         FROM pg_inherits
    #         JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
    #         JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
    #         JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
    #         JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
    #         WHERE parent.relname='{databases_loop}';'''
    #     temp = execute_query(connection,query3,True)
    #     if(len(temp)==0):
    #         no_of_partitions.append(0)
    #     else:
    #         no_of_partitions.append(int(temp))
    query_plan = get_query_plan(connection,query)
    # print(query_plan)

    # query_plan = get_query_plan(connection, query)
    # query_plan[0]['no_of_partitions']=no_of_partitions
    # query_plan[0]['avg_row_length']=avg_row_length
    existing_data=[]
    if os.path.exists("new.json"):
        with open("new.json",'r') as f:
            existing_data = json.load(f)
    # Print or save the JSON formatted query plan
    existing_data.extend(query_plan)
    # query_result = execute_query(connection, query)
    with open("new.json",'w') as f:
        json.dump(existing_data,f,indent=4)
    # Print or process the query result
    
    # Close the database connection
    connection.close()