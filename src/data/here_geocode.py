# This file is for querying coordinates using the Google API
import pandas as pd
import numpy as np
import click
from config import here_app_id, here_app_code # your google api key
import geocoder
import time

def geocode(address, here_app_id, here_app_code):
    '''
   Wraps exception handling around geocode
    '''
    try:
        return geocoder.here(address, app_id=here_app_id, app_code=here_app_code).latlng
    except IndexError:
        return [np.nan, np.nan]

def get_coords(df):
    '''
    Generate latitude,longitude) tuple through Google API query on 
    location (precinct/voter residence) and convert to individual columns.
    Assumes the data is in its final format, i.e. unique row for each voter 
    with valid ddf[address] column.
    Requires config.py with API key (currently suppressed by .gitignore)
    '''
    df['coords'] = df['address'].apply(geocode, args=(here_app_id, here_app_code))
    df[['latitude', 'longitude']] = pd.DataFrame(df['coords'].tolist(), index=df.index)
    return df

def main(directory, name_fmt, file_range, new_fmt='NC-*_coords.tsv'):
    '''For files with the given name format and numbers, get coordinates and write to TSV'''
    for i in file_range: 
        if i < 10:
            i = '00' + str(i)
        elif i < 100:
            i = '0' + str(i)

        start = time.time()
        filename = directory + name_fmt.replace('*', str(i))
        df = pd.read_csv(filename, sep='\t')
        if df.shape[0] == 0:
            continue # Edge case of empty CSV
        df = get_coords(df)
        new_name = directory + new_fmt.replace('*', str(i))
        df.to_csv(new_name, sep='\t')
        print('Geocoded {} in {} min'.format(new_name, (time.time()-start) / 60))

if __name__ == '__main__':
    directory = click.prompt('Directory containing CSVs with addresses',
                          default='../../data/interim/',
                          show_default=True,
                          type=click.Path(exists=True))
    name_fmt = click.prompt('Format of the CSV filenames', 
                            default='NC-*.tsv', 
                            show_default=True, 
                            type=str)
    start = click.prompt('CSV number to start at', 
                        type=int)
    end = click.prompt('CSV to stop at (not inclusive)', 
                        type=int)
    new_fmt = click.prompt('Format for new CSV filenames', 
                            default='NC-*_coords.tsv', 
                            show_default=True, 
                            type=str)

    file_range = range(start, end)    
    main(directory, name_fmt, file_range, new_fmt)
           
