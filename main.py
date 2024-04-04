import psycopg2
import json
import os
# from queries
from connect import connect
from config import load_config

# Function to extract relation names
def extract_relation_names(json_data):
    relation_names = set()  # Using set to ensure unique values
    for item in json_data:
        plan = item.get("Plan", {})
        extract_relation_names_from_plan(plan, relation_names)
    return relation_names

def extract_relation_names_from_plan(plan, relation_names):
    relation_name = plan.get("Relation Name")
    if relation_name:
        relation_names.add(relation_name)
    if "Plans" in plan:
        for subplan in plan["Plans"]:
            extract_relation_names_from_plan(subplan, relation_names)



def get_query_plan(connection, query):
    
    with connection.cursor() as cursor:
        cursor.execute("EXPLAIN (FORMAT JSON) " + query)
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


def main(connection,query):
    # Connect to your PostgreSQL database
    print("Hello,Connection established")
    

    
    
    query_plan = get_query_plan(connection,query)
    relation_names = list(extract_relation_names(query_plan))
    

    avg_row_length = []
    for databases_loop in relation_names:
        query2 = f'''SELECT json_agg(json_build_object(
        'rowsize_in_bytes', rowsize_in_bytes
        )) AS json_output
        FROM (
        SELECT avg(pg_column_size({databases_loop}.*)) as rowsize_in_bytes
        FROM {databases_loop}
        ) AS subquery;'''
        avg_row_length.append(execute_query(connection,query2,True)[0][0][0]['rowsize_in_bytes'])

    no_of_partitions = []

    for databases_loop in relation_names:
        query3 = f'''SELECT
            nmsp_parent.nspname AS parent_schema,
            parent.relname      AS parent,
            nmsp_child.nspname  AS child_schema,
            child.relname       AS child
            FROM pg_inherits
            JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
            JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
            WHERE parent.relname='{databases_loop}';'''
        temp = execute_query(connection,query3,True)
        if(len(temp)==0):
            no_of_partitions.append(0)
        else:
            no_of_partitions.append(int(temp))
    

    query_plan[0]['no_of_partitions']=no_of_partitions
    query_plan[0]['avg_row_length']=avg_row_length
    query_plan[0]['query']=query

    existing_data=[]
    if os.path.exists("new.json"):
        with open("new.json",'r') as f:
            existing_data = json.load(f)
    # Print or save the JSON formatted query plan
    existing_data.extend(query_plan)
    # query_result = execute_query(connection, query)
    with open("new.json",'w') as f:
        json.dump(existing_data,f,indent=4)
