import pandas as pd

#this is for CSV files
"""
pandas Guess the type of var so we can help to him
"""
fileName = 'taxi.csv'
def load_all_csv(file_name):
    time_cols = ['tpep_dropoff_datetime', 'tpep_pickup_datetime']  # for this exempel
    print(pd.read_csv(file_name, parse_dates=time_cols).head())

load_all_csv(fileName)

def load_100_every_time_csv(file_name):
    #to get 100 line every time
    time_cols = ['tpep_dropoff_datetime', 'tpep_pickup_datetime']  # for this exempel
    yield from pd.read_csv(file_name, parse_dates=time_cols, chunksize=100)

def csv_exmple():
    for i, df in enumerate(load_100_every_time_csv(fileName)):
        if i > 10:
            break
        print(len(df))


if __name__ == '__main__':
    csv_exmple()


