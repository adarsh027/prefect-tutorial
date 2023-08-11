import csv
from prefect import task, flow

@task
def extract(path):
    with open(path, 'r') as f:
        text = f.readline().strip()
    data = [int(i) for i in text.split(",")]
    return data

@task
def transform(data):
    data = [i+1 for i in data]
    return data
@task
def load(data, path):
    with open(path, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(data)
    

@flow(name = "my_etl_flow")   
def etl():
    data = extract("values.csv")
    txdata = transform(data)
    load(txdata, 'txvalues.csv')

if __name__ == "__main__":
    etl()    