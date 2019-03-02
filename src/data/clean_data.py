# This file turns raw state data into clean .tsv files that can be used in modeling 

import click
from cleaning_functions import *

def data_to_csv(df, output_directory, filename):
    print('Writing data to {}...'.format(output_directory + filename))
    df.to_csv(output_directory + filename, sep='\t')
    print('Wrote all {} files to {}'.format(filename, output_directory))

def main(state, input_directory, output_directory):
    '''
    Takes given states and directories, runs functions to produce clean .tsv data
    '''
    filename = '{}-*.tsv'.format(state)
    if state == 'NC':
        ddf = merge_NC(input_directory) # Takes forever 

    data_to_csv(ddf, output_directory, filename)

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
                                    default='../../data/processed/NC/',
                                    show_default=True,
                                    type=click.Path(exists=True))    

    main(state, input_directory, output_directory)