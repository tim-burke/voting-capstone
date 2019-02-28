# This file contains functions for cleaning raw state data

import numpy as np
import dask.dataframe as dd
import re

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

def fix_address(row):
    '''
    Takes a dataframe row, returns a single clean address string
    Helper function for clean_NC_12
    '''
    house_number = str(row['house_num'])
    direction = row['street_dir']
    street_name = row['street_name']
    street_type = row['street_type_cd']  
    unstripped_address = '{} {} {} {}'.format(house_number, direction, street_name, street_type)
    address = re.sub('\s+', ' ', unstripped_address).strip()
    return address

def clean_NC_voters_16(input_directory):
    '''
    Cleans the NC voter files ('ncvoter_Statewide.txt'), filtering to active voters.
    Returns a Dask DataFrame of the data
    '''
    file = input_directory + 'ncvoter_Statewide.txt'
    vot_cols = ['ncid', 'voter_status_desc', 'res_street_address', 
    'res_city_desc', 'state_cd', 'zip_code', 'race_code', 'precinct_abbrv', 'precinct_desc']    

    ddf = dd.read_csv(file,
                      sep='\t',
                      blocksize='150MB',
                      encoding="ISO-8859-1",
                      usecols=vot_cols, 
                      dtype={'precinct_abbrv': object,
                      'precinct_desc': object,
                      'ncid': object,
                      'zip_code': object})    
    print('Read in NC voter data')

    # Select rows with active voters only
    ddf['active'] = ddf['voter_status_desc'].apply(is_active, meta=('active', np.float64))
    ddf = ddf.dropna(subset=['active'])
    print('Cleaned NC Voter Data')
    return ddf

def clean_NC_vhist_16(input_directory):
    '''
    Cleans the NC voter history files ('ncvoter_Statewide.txt'),
    creating an election year column and filtering to 2012 and 2016 general elections.
    Returns a Dask DataFrame of the data
    '''    
    file = input_directory + 'ncvhis_Statewide.txt'
    vhist_cols = ['ncid', 'voting_method', 'pct_description', 'pct_label', 'vtd_label', 'election_desc']

    ddf = dd.read_csv(file,
                      sep='\t',
                      blocksize='150MB',
                      encoding="ISO-8859-1",
                      usecols=vhist_cols, 
                      dtype={'ncid': object})
    print('read in NC voter history')

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

def clean_NC_12(input_directory):
    '''
    Cleans the 2012 NC voter data using dask, returning a dask dataframe
    '''
    file = input_directory + 'NC_2012.tsv'
    cols_2012 = ['ncid', 'voter_status_desc', 'house_num','street_dir', 
            'street_name', 'street_type_cd', 'res_city_desc', 'state_cd', 'zip_code', 
            'precinct_abbrv', 'precinct_desc']

    data = dd.read_csv(file, 
                   sep='\t',
                   encoding='UTF-16',
                   blocksize='150MB',
                   usecols=cols_2012,
                   dtype={'precinct_abbrv': object,
                    'precinct_desc': object,
                     'zip_code': object,
                      'ncid': object})
    
    # Filter out bad rows
    data = data.dropna(subset=['precinct_desc'])
    data['active'] = data['voter_status_desc'].apply(is_active, meta=('active', np.float64))
    data = data.dropna(subset=['active'])

    # Create address column
    data['address'] = data.apply(fix_address, axis=1, meta=('address', object))

    # Return relevant columns
    new_cols = ['ncid', 'voter_status_desc', 'house_num', 'address',
            'res_city_desc', 'state_cd', 'zip_code',
            'precinct_abbrv', 'precinct_desc']
    new_names = {'voter_status_desc': 'voter_status_12', 'address': 'address_12', 'voter_status_desc': 'voter_status_12',
             'res_city_desc': 'res_city_desc_12', 'state_cd': 'state_cd_12', 'zip_code': 'zip_code_12',
             'precinct_abbrv': 'precinct_abbrv_12', 'precinct_desc': 'precinct_desc_12'}
    data = data[new_cols]
    data = data.rename(columns=new_names)
    return data

def merge_NC(input_directory, output_directory):
    '''
    Cleans both 2016 and 2012 data, left merges 2012 data onto 2016 to create 
    the final version of the dataset
    '''
    nc16 = clean_NC_16(input_directory).set_index('ncid')
    nc12 = clean_NC_12(input_directory).set_index('ncid')
    ddf = nc16.merge(nc12, how='left', left_index=True, right_index=True)
    return ddf