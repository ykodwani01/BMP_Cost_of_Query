import os
import main  # Assuming main.py contains the main function you want to call
import psycopg2
import json

# from queries
from connect import connect
from config import load_config
folder_path = 'C:/Users/YASH/Downloads/BMP/queries'

def runner():
    config = load_config()
    connection = connect(config)
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    for file_name in files:
        file_path = os.path.join(folder_path, files[0])
        with open(file_path, 'r') as file:
                query_text = file.read()
                
                main.main(connection,query_text)
    connection.close()


runner()

import data 

data.run()
