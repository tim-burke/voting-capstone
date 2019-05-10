import pandas as pd
import numpy as np
import sys
import click
from math import radians, cos, sin, asin, sqrt
from sklearn.model_selection import train_test_split
from nearest_neighbors import haversine, get_slice, k_nearest_dict


def load_data(filepath):
    '''Load finalized data and sort it by rounded lat/long'''
    df = pd.read_csv(filepath, sep='\t')
    df['rounded_lat'] = round(df['latitude'], 2)
    df['rounded_long'] = round(df['longitude'], 2)
    df = df.sort_values(by=['rounded_long', 'latitude'])
    df = df.reset_index(drop=True)
    return df

def find_blocks(df, nearest_dict, k, d):
    '''
    Find the neighbors of each treatment voter, assigning them
    the same block id. 
    Returns indices and block_ids to retrieve a blocked version of 
    the given dataframe
    '''
    blocks = []
    block_ids = []
    b_id = 0
    for t_idx in nearest_dict:
        dists = []
        neighbors = []
        for c_idx in nearest_dict[t_idx]:
            dists.append(haversine(df['latitude'][t_idx], 
                                   df['longitude'][t_idx],
                                   df['latitude'][c_idx], 
                                   df['longitude'][c_idx]))
            neighbors.append(c_idx)
        
        # Sort down to nearby neighbors
        dists = np.array(dists)
        neighbors = np.array(neighbors)
        idxs = np.argsort(dists)[:k]
        dists = dists[idxs]
        neighbors = neighbors[idxs]
        m = np.ma.masked_where(dists > d, neighbors)
        block = np.ma.compressed(m) # Only unmasked elements
        
        # Add the neighbors and t_idx as a block
        if block.shape[0] > 0:
            blocks.extend(block)
            blocks.append(t_idx)
            b_ids = [b_id]*(block.shape[0] + 1)
            block_ids.extend(b_ids)
            b_id += 1
        else:
            continue
    return blocks, block_ids

def main(df, train, treatment, k, d):
    nearest_dict = k_nearest_dict(df, train, treatment, k)
    blocks, block_ids = find_blocks(df, nearest_dict, k, d)
    bdf = generate_blocked_df(df, blocks, block_ids)
    return bdf

def generate_blocked_df(df, blocks, block_ids):
    '''
    Given the indices of voters for each block and associated block ID values, 
    return a blocked dataframe formatted for statsmodels
    '''
    cols = ['ncid', 'poll_changed', 'voted', 'delta_dist', 'voting_method']
    df = df[cols]
    bdf = df.iloc[blocks, :].copy(deep=True)
    bdf['block'] = block_ids
    return bdf

if __name__ == '__main__':
    filepath = click.prompt('Location of finalized data',
                               default='../../data/processed/finalized_data.tsv',
                               show_default=True,
                               type=click.Path(exists=True))
    
    test_size = click.prompt('Proportion of treatment examples to hold out',
                     default=0.3,
                     show_default=True,
                     type=float)
    k = click.prompt('Number of neighbors per voter',
                                   default=50,
                                   show_default=True,
                                   type=int)
    d = click.prompt('Radius of distance caliper around voter',
                                   default=0.2,
                                   show_default=True,
                                   type=float)

    # Load and sample if necessary
    df = load_data(filepath)
    treatment = df.index[df['poll_changed'] == 1].values
    if test_size == 0.0:
        train = treatment
        test = treatment
    else:
        train, test = train_test_split(treatment, test_size=test_size, random_state=1337)

    bdf = main(df, train, treatment, k, d)
    bdf_name = 'df_k{0:d}_d{1:.1f}.tsv'.format(k, d)
    bdf.to_csv('../../data/results/' + bdf_name, sep='\t', index=False)
    print('wrote data to {}'.format('../../data/results/' + bdf_name))