# This file turns raw state data into clean .tsv files that can be used in modeling 

import click
import numpy as np
import dask.dataframe as dd

def get_election_year(election_desc):
    '''Helper function for clean_NC_data'''
    if election_desc == '11/08/2016 GENERAL':
        return 2016
    elif election_desc == '11/06/2012 GENERAL':
        return 2012
    else:
        return np.nan

def clean_NC_data(input_directory, output_directory):
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
                     encoding="ISO-8859-1",
                     usecols=vot_cols, 
                     dtype={'precinct_abbrv': object, 'precinct_desc': object})
    print('read in NC voter data')

    # Voter history dataframe
    vh_file = input_directory + 'ncvhis_Statewide.txt'
    vh = dd.read_csv(vh_file,
                 sep='\t',
                 encoding="ISO-8859-1",
                 usecols=vhist_cols)
    print('read in NC voter history')    

    # Filter voter history to just 2012 and 2016 general
    vh['election_year'] = vh['election_desc'].apply(get_election_year, meta=('election_year', np.float64))
    vh = vh.dropna(subset=['election_year'])

    # Merge and output to the output_directory
    vh = vh.set_index('voter_reg_num')
    vt = vt.set_index('voter_reg_num') 
    data = vt.merge(vh, how='left', left_index=True, right_index=True)
    print('merged NC data; writing to file')

    data.to_csv(output_directory + 'NC/NC-*.tsv', sep='\t')


def main(state='NC', input_directory='../../data/raw/', output_directory='../../data/processed/'):
    '''
    Takes given states and directories, runs functions to produce clean .tsv data
    '''
    if state == 'NC':
        return clean_NC_data(input_directory, output_directory)

if __name__ == '__main__':
    
    input_directory = click.prompt('Input directory containing raw data',
                                   default='../../data/raw/',
                                   show_default=True,
                                   type=click.Path(exists=True))

    state = click.prompt('Which state is the data from?',
                         default='NC',
                         show_default=True,
                         type=str)

    output_directory = click.prompt('Output directory to write processed data',
                                    default='../../data/processed/',
                                    show_default=True,
                                    type=click.Path(exists=True))    

    main(state, input_directory, output_directory)