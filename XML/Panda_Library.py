"""Load rides data from XML"""

import bz2
import xml.etree.ElementTree as xml

import pandas as pd

# Data conversions for example
conversion = [
    ('vendor', int),
    ('people', int),
    ('tip', float),
    ('price', float),
    ('pickup', pd.to_datetime),
    ('dropoff', pd.to_datetime),
    ('distance', float),
]


def read_all(file_name):
    tree = xml.parse(file_name) #to open the xml
    rides = tree.getroot()
    for elem in rides:
        record = {}
        for tag, func in conversion:
            text = elem.find(tag).text
            record[tag] = func(text)
        yield record


def xml_example(file_name):
    records = read_all(file_name)
    return pd.DataFrame.from_records(records)

# Example
if __name__ == '__main__':
    df = xml_example('taxi.xml')
    print(df.dtypes) # to print the type of the struct
    print(df.head()) # to print only the first 5 object
