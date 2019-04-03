# This file is for querying coordinates using the Google API
import pandas as pd
import numpy as np
import click
from config import api_key # your google api key
from pygeocoder import Geocoder as gc
from pygeolib import GeocoderError

def geocode(address, geolocator):
    '''
    Wraps exception handling around pygeocoder b/c apparently that's too much to ask. 
    '''
    try:
        coords = geolocator.geocode(address)
        return coords
    except GeocoderError:
        return np.nan

def get_coords(df):
    '''
    Generate latitude,longitude) tuple through Google API query on 
    location (precinct/voter residence) and convert to individual columns.
    Assumes the data is in its final format, i.e. unique row for each voter 
    with valid ddf[address] column.
    Requires config.py with API key (currently suppressed by .gitignore)
    '''
    geolocator = gc(api_key)
    df['coords'] = df['address'].apply(geocode, args=(geolocator)).apply(lambda x: (x.latitude, x.longitude))
    df[['latitude', 'longitude']] = pd.DataFrame(df['coords'].tolist(), index=df.index)
    return df

def main(directory, name_fmt, file_range, new_fmt='NC-*_coords.tsv'):
    '''For files with the given name format and numbers, get coordinates and write to TSV'''
    for i in file_range: 
        if i < 10:
            i = '0' + str(i)
        filename = directory + name_fmt.replace('*', str(i))
        df = pd.read_csv(filename, sep='\t')
        if df.shape[0] == 0:
            continue # Edge case of empty CSV
        df = get_coords(df)
        new_name = directory + new_fmt.replace('*', str(i))
        df.to_csv(new_name, sep='\t')

if __name__ == '__main__':
    directory = click.prompt('Directory containing CSVs with addresses',
                          default='../../data/interim/',
                          show_default=True,
                          type=click.Path(exists=True))
    name_fmt = click.prompt('Format of the CSV filenames', 
                            default='NC-*.tsv', 
                            show_default=True, 
                            type=str)
    start_at = click.prompt('CSV to start at:', 
                        default=0, 
                        show_default=True, 
                        type=int)
    end_at = click.prompt('CSV to end at:', 
                        default=2, 
                        show_default=True, 
                        type=int)
    new_fmt = click.prompt('Format for new CSV filenames', 
                            default='NC-*_coords.tsv', 
                            show_default=True, 
                            type=str)

    file_range = range(start_at, end_at)    
    main(directory, name_fmt, file_range, new_fmt)
           
