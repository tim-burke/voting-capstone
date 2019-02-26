# This file turns raw state data into clean .tsv files that can be used in modeling 

import click
from cleaning_functions import *

def data_to_csv(df, output_directory, filename):
    print('Writing data to {}...'.format(output_directory + filename))
    df.to_csv(output_directory + filename, sep='\t')
    print('Wrote all {} files to {}'.format(filename, output_directory))

def main(state, year, input_directory, output_directory):
    '''
    Takes given states and directories, runs functions to produce clean .tsv data
    '''
    filename = '{}_{}-*.tsv'.format(state, str(year)[-2:])

    if state == 'NC' and year == 2016:
        data = clean_NC_16(input_directory)
        data_to_csv(data, output_directory, filename)        

    elif state =='NC' and year == 2012:
        data = clean_NC_12(input_directory)
        data_to_csv(data, output_directory, filename)

if __name__ == '__main__':
    
    input_directory = click.prompt('Input directory containing raw data',
                                   default='../../data/raw/',
                                   show_default=True,
                                   type=click.Path(exists=True))

    state = click.prompt('Which state is the data from?',
                         default='NC',
                         show_default=True,
                         type=str)

    year = click.prompt('Which year is the data from?',
                         default=2016,
                         show_default=True,
                         type=int)

    output_directory = click.prompt('Output directory to write processed data',
                                    default='../../data/processed/NC/',
                                    show_default=True,
                                    type=click.Path(exists=True))    

    main(state, year, input_directory, output_directory)