# This file turns raw state data into clean .tsv files that can be used in modeling 

import click
from cleaning_functions import *

def data_to_csv(df, outpath):
    print('Writing data to {}...'.format(outpath))
    df.to_csv(outpath, sep='\t', index=False)
    print('Wrote all files to {}'.format(outpath))

def main(state, filepaths, output_directory, sample=1.0):
    '''
    Takes given states and directories, runs functions to produce clean .tsv data
    '''
    outpath = output_directory + '{}-*.tsv'.format(state)
    if state == 'NC':
        ddf = merge_NC(filepaths) # Takes forever 
    if sample != 1.0:
        ddf = ddf.sample(frac=sample, random_state=1337)

    data_to_csv(ddf, outpath)

if __name__ == '__main__':
    
    # Edit filenames to reflect your local files
    filepaths = {'voters16': 'ncvoter_Statewide.txt',
                 'vhist16': 'ncvhis_Statewide.txt',
                 'voters12': 'NC_2012.tsv', 
                 'polls_12': 'polling_place_20121106.csv',
                 'polls_16': 'polling_place_20161108.csv'}

    input_directory = click.prompt('Input directory containing raw data',
                                   default='../../data/raw/',
                                   show_default=True,
                                   type=click.Path(exists=True))

    output_directory = click.prompt('Output directory to write processed data',
                                default='../../data/interim/',
                                show_default=True,
                                type=click.Path(exists=True)) 

    state = click.prompt('Which state is the data from?',
                         default='NC',
                         show_default=True,
                         type=str)
    
    sample = click.prompt('Proportion of data to output?',
                         default=1.0,
                         show_default=True,
                         type=float)
    
    filepaths = {key: input_directory + filepaths[key] for key in filepaths}

    main(state, filepaths, output_directory, sample)