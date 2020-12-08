"""Load & convert data from CSV file using Python built-in csv module"""
import bz2
import csv
from collections import namedtuple
from datetime import datetime

Column = namedtuple('Column', 'src dest convert') #Another way to make a structure


def parse_timestamp(text):
    return datetime.strptime(text, '%Y-%m-%d %H:%M:%S') #print date time format

# the columns of the file
columns = [
    Column('VendorID', 'vendor_id', int),
    Column('passenger_count', 'num_passengers', int),
    Column('tip_amount', 'tip', float),
    Column('total_amount', 'price', float),
    Column('tpep_dropoff_datetime', 'dropoff_time', parse_timestamp),
    Column('tpep_pickup_datetime', 'pickup_time', parse_timestamp),
    Column('trip_distance', 'distance', float),
]


def read_records(file_name):
    with bz2.open(file_name, 'rt') as fp:  #to open bz2 file
        reader = csv.DictReader(fp)    #to open csv file
        for csv_record in reader:
            record = {}
            for col in columns:   #this is for read all the data columns by colums in one line
                value = csv_record[col.src]
                record[col.dest] = col.convert(value)
            yield record


def example():
    from pprint import pprint

    for i, record in enumerate(read_records('taxi.csv.bz2')):  #this for read line by line
        if i >= 10:
            break
        pprint(record)



if __name__ == '__main__':
    example()
