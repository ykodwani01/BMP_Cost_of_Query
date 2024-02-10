import pandas as pd
import numpy as np
import json

with open("new.json", 'r') as f:
    json_data = json.load(f)

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

# Flatten the JSON data
flat_data = [flatten_json(item) for item in json_data]

# Convert the flattened dictionary to a DataFrame
df = pd.DataFrame(flat_data)

# Print the DataFrame
df.to_csv('output.csv', index=False)

print("DataFrame converted to CSV successfully.")