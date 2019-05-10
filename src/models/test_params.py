import pandas as pd
import numpy as np 
from sklearn.model_selection import train_test_split
from nearest_neighbors import *


if __name__ == '__main__':

    filepath = click.prompt('Filepath to geocoded data',
                                   default='../../data/processed/full_df.csv',
                                   show_default=True,
                                   type=click.Path(exists=True))
    k = click.prompt('Number of neighbors per voter',
                                   default=50,
                                   show_default=True,
                                   type=int)
    test_size = click.prompt('Proportion of treatment examples to hold out',
                     default=0.3,
                     show_default=True,
                     type=float)
    
    distances = np.arange(0.1, 2.1, 0.1)
    
    df = load_and_sort(filepath)
    treatment = df.index[df['poll_changed'] == 1].values
    train, test = train_test_split(treatment, test_size=test_size, random_state=1337)
    
    avg_effects = []
    std_errors = []
    row_counts = []
    dists = []
    
    nearest_dict = k_nearest_dict(df, train, treatment, k)
    for d in distances:
        final_df = make_final_data(df, train, nearest_dict, k, d)
        if final_df is None: # skip if no neighbors found at that distance
            continue
        else:
            avg_effects.append(final_df['ate'].mean())
            row_counts.append(final_df.shape[0])
            dists.append(d)
            SE = np.std(final_df['ate']) / np.sqrt(final_df.shape[0])
            std_errors.append(SE)
    results = pd.DataFrame({'ate': avg_effects, 'se': std_errors, 'n_rows': row_counts, 'distance': dists})
    results.to_csv('../../data/processed/param_test_k{}.csv'.format(k), index=False)
    print('Wrote outcomes to param_test_k{}.csv'.format(k))