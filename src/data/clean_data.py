# This file turns raw state data into clean .tsv files that can be used in modeling 

import click
import dask.dataframe as dd

def get_election_year(election_desc):
    '''Helper function for clean_NC_data'''
    if election_desc == '11/08/2016 GENERAL':
        return 2016
    elif election_desc == '11/06/2012 GENERAL':
        return 2012
    else:
        return np.nan

def clean_NC_data(in_dir, out_dir):
    '''
    Process raw NC voting and voter history data into a standard-format .tsv
    :param in_dir: path to read the raw .txt files
    :param out_dir: path to write the cleaned .tsv files
    '''
    
    # Columns of interest for NC
    vot_cols = ['voter_reg_num', 'voter_status_desc', 'res_street_address', 
    'res_city_desc', 'state_cd', 'zip_code', 'race_code', 'precinct_abbrv', 'precinct_desc']
    vhist_cols = ['voter_reg_num', 'voting_method', 'pct_description', 'pct_label', 'vtd_label', 'election_desc']

    # Registered voter DataFrame
    vt_file = in_dir + 'ncvoter_Statewide.txt'
    vt = dd.read_csv(vt_file,
                     sep='\t',
                     encoding="ISO-8859-1",
                     usecols=vot_cols)

    # Voter history dataframe
    vh_file = in_dir + 'ncvhis_Statewide.txt'
    vh = dd.read_csv(vh_file,
                 sep='\t',
                 encoding="ISO-8859-1",
                 usecols=vhist_cols)

    # Filter voter history to just 2012 and 2016 general
    vh['election_year'] = vh['election_desc'].apply(get_election_year, meta=('election_year', np.float64))
    vh = vh.dropna(subset=['election_year'])

    # Merge and output
    vh = vh.set_index('voter_reg_num')
    vt = vt.set_index('voter_reg_num') # This currently does not work... why? 
    data = vt.merge(vh, how='left', left_index=True, right_index=True)


def main(states='NC', in_dir='../../data/raw/', out_dir='../../data/processed/'):
    '''
    Takes given states and directories, runs functions to produce clean .tsv data
    '''
    if states == 'NC':
        return clean_NC_data

if __name__ == '__main__':
    main()