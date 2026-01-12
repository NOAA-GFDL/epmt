import csv
import pandas as pd
from pprint import pprint
import epmt_query as eq

def parse_annotations(annotations: str):
    """"
    Returns dict parsed from the annotations string
    """
    result = {}
    for pair in annotations.split(";"):
        key, val = pair.split(":", 1) # Take the first colon. Assumption: There shouldn't be more than one.
        result[key.strip()] = val.strip()
    return result


all_jobs = eq.get_jobs(fmt='dict', limit=100) # JK TODO: increase limit once things look okay.
data = []
for job in all_jobs:
    row = parse_annotations(job['annotations']['EPMT_JOB_TAGS'])
    row['cpu_time'] = job['cpu_time']
    data.append(row)


print(f"Writing {len(data)} rows.")
labels = data[0].keys()
with open("jk_data.csv", "w") as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=labels)
    writer.writeheader()
    writer.writerows(data)
