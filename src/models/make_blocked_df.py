import pandas as pd
import numpy as np
import sys
from math import radians, cos, sin, asin, sqrt
from sklearn.model_selection import train_test_split
from nearest_neighbors import haversine, k_nearest_dict

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

def generate_blocked_df(df, blocks, block_ids):
    '''
    Given the indices of voters for each block and associated block ID values, 
    return a blocked dataframe formatted for statsmodels
    '''
    cols = ['ncid', 'poll_changed', 'voted', 'delta_dist']
    df = df[cols]
    bdf = df.iloc[blocks, :].copy(deep=True)
    bdf['block'] = block_ids
    return bdf


