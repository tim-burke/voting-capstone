import click
from math import radians, cos, sin, asin, sqrt
import numpy as np
import pandas as pd

# Distance function
def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points on the earth (specified in decimal degrees)
    """
    
    lat1 = np.abs(lat1)
    lat2 = np.abs(lat2)
    
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    # Radius of earth in kilometers is 6371
    km = 6371* c
    miles = km*0.62
    return miles

def load_and_sort(filepath):
    '''Loading data and dropping duplicates'''
    if '.tsv' in filepath:
        df = pd.read_csv(filepath, sep='\t')
    else:
        df = pd.read_csv(filepath)


    # Adding column for if they voted and removing some unnecessary columns
    df['voted'] = np.where(df['voting_method'].isna(), 0, 1)
    df = df[['ncid', 'race_code', 'voter_status_desc', 'address', 'county_desc', 'polling_place_name', 'voting_method',
            'voted', 'precinct', 'poll_address', 'poll_changed', 'latitude', 'longitude']]

    # Adding column for rounded latitude and rounded longitude, then sorting
    df['rounded_lat'] = round(df['latitude'], 2)
    df['rounded_long'] = round(df['longitude'], 2)
    df = df.sort_values(by=['rounded_long', 'latitude'])


    # Resetting index then getting indices w/ new locations
    df = df.reset_index(drop=True)
    return df

def get_slice(arr, i, k):
    '''Given array, idx of arr, and k, find slice of neighbors'''
    n = arr.shape[0]
    width = int(k/2)
    start = i - width
    end = i + width 
    if width > i:
        start = 0
        end += width - i
    elif end > n:
        start -= end - n
        end = n
    return arr[start:end]

def k_nearest_dict(df, train, treatment, k):
    nearest_dict = {}
    treatment_set = set(treatment)
    idxs = np.array(list(set(df.index) - set(treatment_set)))
    idxs = np.sort(idxs, axis=None) 
    for i in train:
        idx = (np.abs(idxs - i)).argmin() # find closest value's index
        neighbors = get_slice(idxs, idx, k=50) # Slice around it
        nearest_dict[i] = neighbors
    return nearest_dict


def find_neighbors(df, nearest_dict, k):
    '''
    This function creates an array of rows (each of which is another array) to eventually
    be passed into the dataframe 
    '''
    voted_dic = df['voted'].to_dict()
    vote_vecs = []
    dist_vecs = []
    for k1 in nearest_dict:
        dists = []
        voting = []
        for k2 in nearest_dict[k1]:
            dists.append(haversine(df['latitude'][k1], df['longitude'][k1], df['latitude'][k2], df['longitude'][k2]))
            voting.append(voted_dic[k2])
        dists = np.array(dists)
        voting = np.array(voting)
        idxs = np.argsort(dists)[:k]
        dists = dists[idxs]
        voting = voting[idxs]
        
        # Keep sorted arrays, padded with NaN if less then k length
        dists = np.pad(dists, (0, k - len(dists)), 'constant', constant_values=(np.inf))
        voting = np.pad(voting, (0, k - len(voting)), 'constant', constant_values=(np.nan))

        vote_vecs.append(voting)
        dist_vecs.append(dists)
    
    return np.vstack(vote_vecs), np.vstack(dist_vecs) 


def calc_y0(votes, dists, d):
    m = np.ma.masked_where(dists > d, votes) # mask values outside distance range
    result = m.mean(axis=1)
    return result

def make_final_data(df, train, nearest_dict, k, d):
    '''
    Return a DataFrame with the treatment group
    '''
    
    votes, dists = find_neighbors(df, nearest_dict, k)
    final_df = df.iloc[train][['ncid', 'poll_changed']].reset_index(drop=True)

    # Get the final results
    control = calc_y0(votes, dists, d)
    final_df = final_df[~control.mask]
    if final_df.shape[0] == 0: # Edge case of no treatment voters have neighbors
        return None
    final_df['y0'] = control[~control.mask]
    final_df = final_df.merge(df[['ncid', 'voted']], how='inner', on='ncid').rename({'voted': 'y1'}, axis=1)
    final_df['ate'] = final_df['y1'] - final_df['y0']
    return final_df

if __name__ == '__main__':
    filepath = click.prompt('Filepath to geocoded data',
                                   default='../../data/processed/finalized_data.tsv',
                                   show_default=True,
                                   type=click.Path(exists=True))
    k = click.prompt('Number of neighbors per voter',
                                   default=50,
                                   show_default=True,
                                   type=int)
    d = click.prompt('Neighborhood size?',
                     default=0.2,
                     show_default=True,
                     type=float)

    df = load_and_sort(filepath)
    treatment = df.index[df['poll_changed'] == 1].values
    
    # train set option is for test_params when some data are held out
    train = treatment 

    # Find neighbors and calculate ATE
    nearest_dict = k_nearest_dict(df, train, treatment, k)
    final_df = make_final_data(df, train, nearest_dict, k, d)
    print('ATE of this sample calculated as {}'.format(final_df['ate'].mean()))
    final_df.to_csv('../../data/processed/NC_final.tsv', sep='\t')