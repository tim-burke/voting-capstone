import numpy as np
import pandas as pd
import dask.dataframe as dd
from csv import QUOTE_NONE
import click

def is_treatment(ncid, s):
    """Takes NCID and set of treatment NCIDs, returns binary for membership"""
    if ncid in s:
        return 1.
    else:
        return np.nan

def fix_poll_changed(row, bad_ids):
    '''Change the treatment assignment by removing bad ids'''
    if row['ncid'] in bad_ids:
        return 0
    else:
        return row['poll_changed']

def main(filepath, outfile):
    # Load in flawed treatment file, 2012 voter registration
    df = pd.read_csv(filepath)
    cols_2012 = ['ncid', 'voter_status_desc', 'house_num', 'street_dir', 
            'street_name', 'street_type_cd', 'res_city_desc', 'state_cd', 'zip_code', 
            'precinct_abbrv', 'precinct_desc']
    ddf = dd.read_csv('../../data/raw/VR_Snapshot_20121106.txt',
                   sep='\t',
                   encoding='UTF-16',
                   blocksize=150000000,
                   usecols=cols_2012, 
                   quoting=QUOTE_NONE,
                   dtype={'precinct_abbrv': object,
                    'precinct_desc': object,
                     'zip_code': object, 
                     'house_num': int,
                     'ncid': object})

    # Final treatment set, filter 2012 data down to treatment only
    s = set(df[df['poll_changed'] == 1]['ncid'].unique())
    ddf['poll_change'] = ddf['ncid'].apply(is_treatment, args=(s,), meta=('poll_change', np.float64))
    ddf = ddf.dropna(subset=['poll_change'])
    ddf = ddf.compute() # Dask df -> pd df

    # Identify cases where treatment voters were removed, precinct was empty str
    ddf['bad_precinct'] = ddf['precinct_abbrv'].astype(str).apply(lambda x: 1 if x.strip() == '' else 0)
    bad_ids = set(ddf[ddf['bad_precinct'] == 1]['ncid'].unique())

    # Set previously removed voters to not be in treatment group
    df['poll_changed'] = df.apply(fix_poll_changed, args=(bad_ids,), axis=1)

    # Clean up and output
    df = df.drop(['Unnamed: 0', 'Unnamed: 0.1'], axis=1)
    df.to_csv('../../data/processed/' + outfile, index=False)

if __name__ == '__main__':    
    filepath = click.prompt('Filepath to full_df.csv',
                                   default='../../data/interim/full_df.csv',
                                   show_default=True,
                                   type=click.Path(exists=True))
    outfile = click.prompt('Name to write new file to',
                           default='fixed_full_df.csv',
                           show_default=True,
                           type=str)
    main(filepath, outfile)


