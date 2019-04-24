import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
# import sys
from nearest_neighbors import *



if __name__ == '__main__':

    filepath = click.prompt('Filepath to geocoded data',
                                   default='../../data/interim/full_df.csv',
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
    
    distances = list(np.arange(0.1, 2., 0.1))
    
    df = load_and_sort(filepath)
    treatment = df.index[df['poll_changed'] == 1].values
    train, test = train_test_split(treatment, test_size=test_size, random_state=1337)
    
    avg_effects = []
    std_errors = []
    row_counts = []
    for d in distances:
        final_df = make_final_data(df, train, treatment, k, d)
        # Do stuff here to measure ATE, SE, nrows...? 