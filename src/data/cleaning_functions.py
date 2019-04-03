# This file contains functions for cleaning raw state data

import numpy as np
import dask.dataframe as dd
import pandas as pd
import re
import csv

def get_election_year(election_desc):
    '''
    Apply to election_desc column
    Helper function for clean_NC_16
    '''
    if election_desc == '11/08/2016 GENERAL':
        return 2016
    elif election_desc == '11/06/2012 GENERAL':
        return 2012
    else:
        return np.nan

def is_active(voter_status_desc):
    '''
    Apply to voter_status_desc column, returns nan for inactive
    Helper function to clean_NC_12
    '''
    if voter_status_desc == 'ACTIVE':
        return 1.0
    else:
        return np.nan

def changed_polling_location(row):
    """
    Indicates whether someone was moved to a different
    polling place between 2012 general election and 2016 general election.
    """
    if str(row['precinct_abbrv']).strip() != str(row['precinct_abbrv_12']).strip():
        return 1
    else:
        return 0

def find_house_num(address):
    '''
    Helper function to extract the house number from the address
    Apply to the address row
    '''
    nums = re.findall('\d+', address)
    if nums:
        return int(nums[0])
    else:
        return -1

def house_num_match(row):
    '''
    Returns 1 if house # matches, else NaN. 
    Apply to DataFrame rows
    '''
    h12 = row['house_num_12']
    h16 = row['house_num_16']
    if h16 == h12:
        return 1.0
    else:
        return np.nan

def poll_address(row):
    '''
    Fixes address format for polling place data
    Helper function to clean_NC_polling
    '''
    house_number = str(row['house_num'])
    street_name = row['street_name']
    city_name = row['city']
    state_cd = row['state']
    zipcode = row['zip']
    unstripped_address = '{} {}, {}, {} {}'.format(house_number, street_name, city_name, state_cd, zipcode)
    address = re.sub('\s+', ' ', unstripped_address).strip()
    return address

def fix_address(row):
    '''
    Takes a dataframe row, returns a single clean address string
    Helper function for clean_NC_12
    Update: Add fields for Google API queries
    '''
    house_number = str(row['house_num'])
    direction = row['street_dir']
    street_name = row['street_name']
    street_type = row['street_type_cd']  
    city_name = row['res_city_desc']
    state_cd = row['state_cd']
    zipcode = row['zip_code']

    unstripped_address =  '{} {} {} {},{},{} {}'.format(house_number, direction, street_name, street_type, city_name, state_cd, zipcode)
    address = re.sub('\s+', ' ', unstripped_address).strip()
    return address

def de_duplicate_ncid(ddf):
    '''
    Slow workaround to drop all duplicated NCIDs in vhist.
    Necessary because Dask has not implemented this behavior yet
    '''

    # Eliminate duplicates, voters who only were in one election
    counts = ddf.groupby('ncid')['election_year'].count().compute()
    mask = counts.values != 2
    s = set(counts.index[mask]) # set of duplicated ncids
    ddf = ddf.set_index()

def clean_NC_voters_16(filepath):
    '''
    Cleans the NC voter files, filtering to active voters.
    Returns a Dask DataFrame of the data
    '''
    vot_cols = ['ncid', 'voter_status_desc', 'res_street_address', 
                'race_code', 'precinct_abbrv', 'precinct_desc']    

    ddf = dd.read_csv(filepath,
                      sep='\t',
                      blocksize=150000000,
                      encoding="ISO-8859-1",
                      usecols=vot_cols, 
                      dtype={'precinct_abbrv': object,
                      'precinct_desc': object,
                      'ncid': object,
                      'zip_code': object})    
    print('Read in NC Voter Data')

    # Select rows with active voters only
    ddf['active'] = ddf['voter_status_desc'].apply(is_active, meta=('active', np.float64))
    ddf = ddf.dropna(subset=['active'])
    ddf['house_num_16'] = ddf['res_street_address'].apply(find_house_num, meta=('house_num_16', int))
    ddf = ddf.drop('res_street_address', axis=1)
    print('Cleaned NC Voter Data')
    return ddf

def clean_NC_vhist_16(filepath):
    '''
    Cleans the NC voter history files ('ncvoter_Statewide.txt'),
    creating an election year column and filtering to 2012 and 2016 general elections.
    Returns a Dask DataFrame of the data
    '''    
    vhist_cols = ['ncid', 'voting_method', 'pct_description', 'pct_label',
     'vtd_label', 'election_desc', 'county_desc']

    ddf = dd.read_csv(filepath,
                      sep='\t',
                      blocksize=150000000,
                      encoding="ISO-8859-1",
                      usecols=vhist_cols, 
                      dtype={'ncid': object})
    print('Read in NC Voter History')

    # Filter to just 2016 and 2012 General elections
    ddf['election_year'] = ddf['election_desc'].apply(get_election_year, meta=('election_year', np.float64))
    ddf = ddf.dropna(subset=['election_year'])
    print('Cleaned NC Voter History Data')
    return ddf

def merge_NC_16(voters, vhist):
    '''
    Takes cleaned NC registered voters, voter history Dask DataFrames.
    Returns Dask DataFrame of voter history left-merged onto the voter data. 
    '''
    voters = voters.set_index('ncid')
    vhist = vhist.set_index('ncid')
    ddf = voters.merge(vhist, how='left', left_index=True, right_index=True) 
    print('Finished merging NC 2016 data')
    return ddf

def clean_NC_12(filepath):
    '''
    Cleans the 2012 NC voter data using dask, returning a dask dataframe
    '''
    cols_2012 = ['ncid', 'voter_status_desc', 'house_num', 'street_dir', 
            'street_name', 'street_type_cd', 'res_city_desc', 'state_cd', 'zip_code', 
            'precinct_abbrv', 'precinct_desc']

    data = dd.read_csv(filepath, 
                   sep='\t',
                   encoding='UTF-16',
                   blocksize=150000000,
                   usecols=cols_2012, 
                   quoting=csv.QUOTE_NONE,
                   dtype={'precinct_abbrv': object,
                    'precinct_desc': object,
                     'zip_code': object, 
                     'house_num': int,
                     'ncid': object})
    
    # Filter out bad rows
    data = data.dropna(subset=['precinct_desc'])
    data['active'] = data['voter_status_desc'].apply(is_active, meta=('active', np.float64))
    data = data.dropna(subset=['active'])

    # Create address column
    data['address'] = data.apply(fix_address, axis=1, meta=('address', object))

    # Return relevant columns
    new_cols = ['ncid', 'house_num', 'address', 'precinct_abbrv']
    new_names = {'house_num': 'house_num_12', 'precinct_abbrv': 'precinct_abbrv_12'}
    data = data[new_cols]
    data = data.rename(columns=new_names)
    return data

def clean_NC_polling(filepaths):
    '''
    Cleans the polling place data for North Carolina, returns pd DataFrame
    '''
    p12 = pd.read_csv(filepaths['polls_12'], 
                      encoding='utf-16',
                      sep='\t', 
                      dtype={'zip': object})
    p16 = pd.read_csv(filepaths['polls_16'], 
                      encoding='utf-16',
                      sep='\t', 
                      dtype={'zip': object})
    p12['election_year'] = 2012.
    p16['election_year'] = 2016.
    pp = pd.concat((p16, p12))
    pp['city'] = pp['city'].fillna('CONWAY') # Correct missing data
    pp['poll_address'] = pp.apply(poll_address, axis=1)
    pp = pp.drop(['election_dt', 'house_num', 'street_name', 
                 'city', 'state', 'zip', 'polling_place_id'], axis=1)
    return pp

def de_duplicate_vhist(ddf):
    '''
    Takes dask dataframe, removes NCID duplicates. 
    Used for Voter history, so assumes that bad NCIDs are those that appear
    more than twice (duplicates) or less (voters without records in both elections).
    Return: ddf with NCID as index, duplicates removed.  
    '''
    counts = ddf.groupby('ncid')['election_year'].count().compute()
    mask = counts.values == 2
    s = set(counts.index[mask])
    ddf = ddf.set_index('ncid')
    ddf = ddf.map_partitions(lambda x: x[x.index.isin(s)], 
                             meta=dict(ddf.dtypes))
    print('Removed duplicate NCID cases')
    return ddf

def merge_NC(filepaths):
    '''
    Cleans 2016 voter data, inner merges to 2012 voter data,
    filters to voters who did not move. 
    filepaths is a dict of paths to each voter file
    '''
    nc16 = clean_NC_voters_16(filepaths['voters16']).set_index('ncid')
    nc12 = clean_NC_12(filepaths['voters12']).set_index('ncid')
    ddf = nc16.merge(nc12, how='inner', left_index=True, right_index=True)
    ddf['poll_changed'] = ddf.apply(changed_polling_location, axis=1, meta=('poll_changed', int))
    
    # Filter to only voters whose address didn't change b/t '12 & '16
    ddf['address_match'] = ddf.apply(house_num_match, axis=1, meta=('address_match', float))
    ddf = ddf.dropna(subset=['address_match'])
    ddf = ddf.drop(['address_match', 'precinct_abbrv', 'precinct_abbrv_12'], axis=1)

    # Merge on cleaned voter histories
    vhist = clean_NC_vhist_16(filepaths['vhist16'])
    vhist = de_duplicate_vhist(vhist)
    ddf = ddf.merge(vhist, how='inner', left_index=True, right_index=True)

    # Fix minor data inconsistencies in precincts
    ddf['precinct'] = ddf['precinct_desc'].combine_first(ddf['pct_description'])
    ddf['precinct'] = ddf['precinct'].mask(ddf['precinct'] == 'GLOBE/JOHNS RIVER/MULBERRY/WILSON CREEK',
                                                                 'GLOBE/JOHNS RIVER/MULBERRY/WIL')
    
    # Merge on precincts
    ddf = ddf.reset_index()
    pp = clean_NC_polling(filepaths)
    ddf = ddf.merge(pp,
                   how='inner',
                   left_on=['county_desc', 'precinct', 'election_year'],
                   right_on=['county_name', 'precinct_name', 'election_year'])
    ddf = ddf.drop(['pct_description', 'precinct_desc', 'precinct_name', 'house_num_16', 'house_num_12'], axis=1)
    return ddf

def sample_by_NCID(ddf, frac):
    '''
    Randomly sample from a dask dataframe by NCID s.t. all occurences of
    each NCID will appear in the sample. 
    Assumes the data are in their final format, i.e. 2 copies of each NCID. 
    '''
    unique = ddf.drop_duplicates(subset=['ncid'], keep='first') # get unique NCIDs
    ncids = set(unique['ncid'].sample(frac=0.05).compute().values) # place them in a set
    ddf = ddf.map_partitions(lambda x: x[x.ncid.isin(s)], meta=dict(ddf.dtypes))
    return ddf