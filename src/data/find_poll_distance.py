import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import dask.dataframe as dd
from nearest_neighbors import haversine
from csv import QUOTE_NONE
import click

def filter_by_membership(ncid, s):
    """Takes NCID and a set of NCIDs, returns 1 if NCID in set"""
    if ncid in s:
        return 1.
    else:
        return np.nan

def find_poll_distances(geo_path, path_12, path_poll):
    
    # Load in the polling data
    polling_coords = pd.read_csv(path_poll, sep ='\t')
    polling_coords_2012 = polling_coords[polling_coords['election_year']==2012]
    polling_coords_2012 = polling_coords_2012.rename({'latitude': 'poll_lat_12',
                                                      'longitude': 'poll_long_12',
                                                      'address': 'poll_address'}, axis=1)
    polling_coords_2016 = polling_coords[polling_coords['election_year']==2016]
    polling_coords_2016 = polling_coords_2016.rename({'latitude': 'poll_lat_16',
                                                      'longitude': 'poll_long_16',
                                                      'address': 'poll_address'}, axis=1)

    # Load in Geocoded data 2016 data, merge on poll coordinates
    nc16 = pd.read_csv('../data/processed/fixed_full_df.csv', dtype={'ncid': object})
    polling_coords_2016 = polling_coords_2016[['poll_address', 'poll_lat_16', 'poll_long_16']].drop_duplicates(subset='poll_address')
    nc16 = nc16.merge(polling_coords_2016, how='inner', on='poll_address')

    # Load in 2012 data into Dask, merge on coordinates
    cols_2012 = ['ncid','county_desc','precinct_abbrv','precinct_desc', 'voter_status_desc']
    df_voter2012 = dd.read_csv('../data/raw/VR_Snapshot_20121106.txt', 
                   sep='\t',
                   encoding='UTF-16',
                   blocksize=150000000,
                   usecols=cols_2012, 
                   quoting=QUOTE_NONE,
                   dtype={'precinct_abbrv': object,
                    'precinct_desc': object,
                     'ncid': object})
    df_voter2012 = df_voter2012.dropna(subset=['precinct_desc'])
    polling_coords_2012 = polling_coords_2012[['county_name', 'precinct_name', 'poll_lat_12', 'poll_long_12']]
    merge_NC_2012 = df_voter2012.merge(polling_coords_2012,
                                   left_on=['county_desc','precinct_desc'],
                                   right_on=['county_name','precinct_name'],
                                   how = 'left')
    
    # Filter the 2012 data to NCIDs in our geocoded data
    s = set(nc16['ncid'].unique())
    merge_NC_2012['relevant'] = merge_NC_2012['ncid'].apply(filter_by_membership, args=(s,), meta=('relevant', np.float64))
    merge_NC_2012 = merge_NC_2012.dropna(subset=['relevant']).drop('relevant', axis=1)
    merge_NC_2012 = merge_NC_2012.compute() # Convert to pd DataFrame
    
    # Clean specific cases of bad values
    merge_NC_2012 = merge_NC_2012.dropna(subset=['precinct_abbrv'])
    merge_NC_2012.loc[(merge_NC_2012['ncid'].isin(['CW207902', 'CW664395'])) & (merge_NC_2012['county_desc'] == 'MECKLENBURG'), 'ncid'] = np.nan
    merge_NC_2012 = merge_NC_2012.dropna(subset=['ncid'])
    merge_NC_2012 = merge_NC_2012.drop_duplicates(subset=['ncid'], keep='first')
    201_case = polling_coords_2012[(polling_coords_2012['precinct_name'].str.contains('201')) & (polling_coords_2012['county_name'] == 'MECKLENBURG')]
    201_vals = 201_case[['poll_lat_12', 'poll_long_12']].values
    merge_NC_2012.loc[merge_NC_2012['poll_lat_12'].isna(), ['poll_lat_12', 'poll_long_12']] = 201_vals

    # Merge 2012 coords onto 2016 data
    final = nc16.merge(merge_NC_2012[['ncid', 'poll_lat_12', 'poll_long_12']], on='ncid', how='left')

    # Coalesce 2012 coords with 2016 if na - all such cases did not change polling pl.
    p_lat_12 = final.loc[:, 'poll_lat_12'].combine_first(final.loc[:, 'poll_lat_16']).values
    p_long_12 = final.loc[:, 'poll_long_12'].combine_first(final.loc[:, 'poll_long_16']).values
    final.loc[:, 'poll_lat_12'] = p_lat_12
    final.loc[:, 'poll_long_12'] = p_long_12

    # Calculate the distance to 2012 and 2016 polls, change in distance
    final['poll_dist_12'] = final.apply(lambda x: haversine(x['poll_lat_12'], 
                                                          x['poll_long_12'], 
                                                          x['latitude'], 
                                                          x['longitude']), axis=1)
    final['poll_dist_16'] = final.apply(lambda x: haversine(x['poll_lat_16'], 
                                                            x['poll_long_16'], 
                                                            x['latitude'], 
                                                            x['longitude']), axis=1)
    final['delta_dist'] = final['poll_dist_16'] - final['poll_dist_12']

    # Force unchanged polls to be exactly 0 distance 
    final['delta_dist'] = np.where(final['poll_changed'] == 0, 0., final['delta_dist'])

    # If treatment voter moved 0mi from old poll, move them to control
    final['poll_changed'] = np.where(np.isclose(final['delta_dist'], [0.]), 0, final['poll_changed'])
    return final

if __name__ == '__main__':
    
    geo_path = click.prompt('Geocoded Dataframe Location',
                               default='../../data/processed/fixed_full_df.csv',
                               show_default=True,
                               type=click.Path(exists=True))
    path_12 = click.prompt('2012 Voter Registration Data Location',
                               default='../../data/raw/VR_Snapshot_20121106.txt',
                               show_default=True,
                               type=click.Path(exists=True))
    path_poll = click.prompt('Polling Coordinates Location',
                               default='../../data/processed/all_polling_coords.tsv',
                               show_default=True,
                               type=click.Path(exists=True))
    outname = click.prompt('Output File Name',
                               default='finalized_data.tsv',
                               show_default=True,
                               type=click.Path(exists=True))
    
    final = find_poll_distances(geo_path, path_12, path_poll)
    final.to_csv('../../data/processed/' + outname, index=False, sep='\t')


