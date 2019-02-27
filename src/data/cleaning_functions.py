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
    Apply to voter_status_desc column
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

def clean_NC_12(input_directory):
    '''
    Cleans the 2012 NC voter data using dask, returning a dask dataframe
    '''
    file = input_directory + 'NC_2012.tsv'
    cols_2012 = ['voter_reg_num', 'voter_status_desc', 'house_num','street_dir', 
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
                      'voter_reg_num': object})
    # Filter out bad rows
    # data = data[data['precinct_desc'].notnull()].compute()
    data = data.dropna(subset=['precinct_desc'])
    data['active'] = data['voter_status_desc'].apply(is_active, meta=('active', np.float64))
    data = data.dropna(subset=['active'])

    # Create address column
    data['address'] = data.apply(fix_address, axis=1, meta=('address', object))

    # Return relevant columns
    new_cols = ['voter_reg_num', 'voter_status_desc', 'house_num', 'address',
            'res_city_desc', 'state_cd', 'zip_code',
            'precinct_abbrv', 'precinct_desc']
    new_names = {'voter_status_desc': 'voter_status_12', 'address': 'address_12', 'voter_status_desc': 'voter_status_12',
             'res_city_desc': 'res_city_desc_12', 'state_cd': 'state_cd_12', 'zip_code': 'zip_code_12',
             'precinct_abbrv': 'precinct_abbrv_12', 'precinct_desc': 'precinct_desc_12'}
    data = data[new_cols]
    data = data.rename(columns=new_names)
    # data = dd.from_pandas(data, npartitions=50) # Deals with odd Dask behavior
    return data

def clean_NC_16(input_directory):
    '''
    Process raw NC voting and voter history data into a standard-format .tsv
    :param input_directory: path to read the raw .txt files
    :param output_directory: path to write the cleaned .tsv files
    '''
    
    # Columns of interest for NC
    vot_cols = ['voter_reg_num', 'voter_status_desc', 'res_street_address', 
    'res_city_desc', 'state_cd', 'zip_code', 'race_code', 'precinct_abbrv', 'precinct_desc']
    vhist_cols = ['voter_reg_num', 'voting_method', 'pct_description', 'pct_label', 'vtd_label', 'election_desc']

    # Registered voter DataFrame
    vt_file = input_directory + 'ncvoter_Statewide.txt'
    vt = dd.read_csv(vt_file,
                     sep='\t',
                     blocksize='150MB',
                     encoding="ISO-8859-1",
                     usecols=vot_cols, 
                     dtype={'precinct_abbrv': object,
                      'precinct_desc': object, 'voter_reg_num': object, 'zip_code': object})    
    print('read in NC voter data')

    # Voter history dataframe
    vh_file = input_directory + 'ncvhis_Statewide.txt'
    vh = dd.read_csv(vh_file,
                 sep='\t',
                 blocksize='150MB',
                 encoding="ISO-8859-1",
                 usecols=vhist_cols, 
                 dtype={'voter_reg_num': object})
    print('read in NC voter history')

    # Filter voter data to relevant elections, active voters only
    vh['election_year'] = vh['election_desc'].apply(get_election_year, meta=('election_year', np.float64))
    vh = vh.dropna(subset=['election_year'])
    vt['active'] = vt['voter_status_desc'].apply(is_active, meta=('active', np.float64))
    vt = vt.dropna(subset=['active'])
    # vh = vh[vh['election_year'].notnull()].compute() # Why does this convert to pd Dataframe? 
    # vt = vt.loc[vt['voter_status_desc'] == 'ACTIVE'].compute()


    # Merge data and return
    vh = vh.set_index('voter_reg_num')
    vt = vt.set_index('voter_reg_num')
    data = vt.merge(vh, how='left', left_index=True, right_index=True) 
    # data = dd.from_pandas(data, npartitions=50) # Deals with odd Dask behavior
    print('Finished merging NC 2016 data')
    return data

def merge_NC(input_directory, output_directory):
    '''
    Cleans both 2016 and 2012 data, left merges 2012 data onto 2016 to create 
    the final version of the dataset
    '''
    nc16 = clean_NC_16(input_directory)
    nc12 = clean_NC_12(input_directory)
    return