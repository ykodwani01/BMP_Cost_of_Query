import pandas as pd
import json

def run():
    try:
        with open("new.json", 'r') as f:
            json_data = json.load(f)
    except FileNotFoundError:
        print("Error: File 'new.json' not found.")
        return
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in 'new.json'.")
        return

    # Flatten the JSON data
    flat_data = [flatten_json(item) for item in json_data]

    if not flat_data:
        print("Error: No data found in JSON file.")
        return

    # Convert the flattened dictionary to a DataFrame
    df = pd.DataFrame(flat_data)

    # Save the DataFrame to a CSV file
    csv_file_path = 'output.csv'
    df.to_csv(csv_file_path, index=False)

    print("DataFrame converted to CSV successfully. Saved as:", csv_file_path)

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], name + a + '_')
        elif isinstance(x, list):
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

run()
