import click
from math import radians, cos, sin, asin, sqrt
import numpy as np
import pandas as pd
from pygeocoder import Geocoder as geo
from tqdm import tqdm_notebook as tqdm



########################################################################
####################### Data imports and cleaning ######################
########################################################################


# Loading data and dropping duplicates
df = pd.read_csv('NC_geocoded_csv')


# Adding column for if they voted and removing some unnecessary columns
df['voted'] = np.where(df['voting_method'].isna(), 0, 1)
df = df[['ncid', 'race_code', 'voter_status_desc', 'address', 'county_desc', 'polling_place_name', 'voting_method',
        'voted', 'precinct', 'poll_address', 'poll_changed', 'latitude', 'longitude']]



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



# Adding column for rounded latitude and rounded longitude, then sorting
df['rounded_lat'] = round(df['latitude'], 2)
df['rounded_long'] = round(df['longitude'], 2)
df = df.sort_values(by=['rounded_long', 'latitude'])


# Resetting index then getting indices w/ new locations
df = df.reset_index()
new_poll_indeces = df.index[df['poll_changed'] == 1].tolist()


# Creating dataframe with NCID and indices
def ncid_to_index ():
    '''
    Creates a dataframe that maps NCID to that individual's index. Used in later cleaning.
    '''
    ncid_list = []
    for count, elem in enumerate (new_poll_indeces):
        ncid_list.append(df['ncid'][count])

    return pd.DataFrame({'ncid': ncid_list, 'index': new_poll_indeces})

ncid_to_index_df = ncid_to_index()
index_voted_df = df['voted']





########################################################################
####################### Building 50-person matrix ######################
########################################################################


def fifty_nearest (new_poll_indeces):
    '''
    Params:
        new_poll_indices_set: set of indices where that voter's polling location changed 
    
    Returns:
        A dictionary where each key is the index of a voter whose poll changed and each
        value is a list of indices of his 50 closest neighbors and whether that person voted
    
    Note:
        This only returns indices of neighbors who did not have their location changed, 
        and does not return the individual himself. 
    '''
    
    nearest_dict = {}
    new_poll_indices_set = set(new_poll_indeces)

    for elem in new_poll_indeces:
        unfiltered_list = np.linspace(elem - 34, elem + 35, 70)
        removed_movers_list = [elem for elem in unfiltered_list if elem not in new_poll_indices_set]
        nearest_dict[elem] = removed_movers_list[11:61]

    return nearest_dict 




# Note to Tim and Aakriti: if you want to run 200 nearest, uncomment this function and run that 
# when instead of fifty_nearest() when creating nearest_dic below. Also see the manual key setting
# for 0, 4, and 87, and change the argument when running generate_column_names() 


# def twohundred_nearest (new_poll_indeces):
#     '''
#     Params:
#         new_poll_indices_set: set of indices where that voter's polling location changed 
    
#     Returns:
#         A dictionary where each key is the index of a voter whose poll changed and each
#         value is a list of indices of his 50 closest neighbors and whether that person voted
    
#     Note:
#         This only returns indices of neighbors who did not have their location changed, 
#         and does not return the individual himself. 
#     '''
    
#     nearest_dict = {}
#     new_poll_indices_set = set(new_poll_indeces)

#     for elem in new_poll_indeces:
#         unfiltered_list = np.linspace(elem - 114, elem + 115, 230)
#         removed_movers_list = [elem for elem in unfiltered_list if elem not in new_poll_indices_set]
#         nearest_dict[elem] = removed_movers_list[11:211]

#     return nearest_dict



# Creating the dict
nearest_dict = fifty_nearest(new_poll_indeces)

# Have to manually update for some since they're within 50 of the end 
nearest_dict[0] = [elem for elem in range(50)]
nearest_dict[4] = [elem for elem in range(50)]


# # Comment out lines above and uncomment this if running it within 200  
# nearest_dict[0] = [elem for elem in range(200)]
# nearest_dict[4] = [elem for elem in range(200)]
# nearest_dict[87] = [elem for elem in range(200)]



def generate_column_names (n):
    '''
    Simple function to generate column names for the n closest neighbors to be used in distance matrix
    '''
    col_names = []
    for elem_one in range(n):
        for elem_two in range(2):
            if elem_two == 0:
                col_names.append("Neighbor" + str(elem_one+1) + "Distance")
            else:
                col_names.append("Neighbor" + str(elem_one+1) + "Voted")
    
    return col_names



def generate_rows ():
    '''
    This function creates an array of rows (each of which is another array) to eventually
    be passed into the dataframe 
    '''
    list_of_rows = []
    for key in nearest_dict.keys():

        row_values = []
        for count, elem in enumerate(nearest_dict[key]):
            row_values.append(haversine(df['latitude'][key], df['longitude'][key], df['latitude'][elem], df['longitude'][elem]))
            current_index = nearest_dict[key][count] 
            row_values.append(index_voted_df[current_index])
        list_of_rows.append(row_values)
        row_values = []
    
    return list_of_rows  


# Generating the rows for the dataframe
rows_for_df = generate_rows()


# Creating the dataframe and adding a column for the initial index
neighbor_df = pd.DataFrame(rows_for_df, columns = generate_column_names(50))
neighbor_df['initial_index'] = new_poll_indeces


# Merging with other df to get ncid column 
final_df = pd.merge(ncid_to_index_df, neighbor_df, left_on='index', right_on='initial_index', how='inner').drop(['initial_index'], axis=1)



# Adding column for whether the treated individual voted, then reordering columns 
final_df['voted'] = [index_voted_df[elem] for elem in final_df['index']] 

cols = list(final_df)
reordered_cols = cols.insert(0, cols.pop(cols.index('voted')))
final_df = final_df.loc[:, cols]

cols = list(final_df)
reordered_cols = cols.insert(0, cols.pop(cols.index('ncid')))
final_df = final_df.loc[:, cols]
final_df = final_df.drop(['index'], axis=1)





########################################################################
################# K nearest neighbors with max distance ################
########################################################################


# Removing a numpy warning about nulls
np.warnings.filterwarnings('ignore')



def k_empty_df (n_neighbors):
    '''
    Generates an empty dataframe with k columns based on input param  
    '''
    column_list = generate_column_names(n_neighbors)
    column_list.append('ncid')
    column_list.append('voted')
    column_list.insert(0, column_list.pop(column_list.index('ncid'))) 
    column_list.insert(0, column_list.pop(column_list.index('voted'))) 
    
    return pd.DataFrame(columns=column_list)




def control_set (max_distance, k_neighbors):
    '''
    Input max distance/number of desired neighborsand this generates the control set df 
    '''
    
    # Creating empty df that will be populated row-by-row
    k_nearest_df = k_empty_df(k_neighbors)
    
    
    for index in range(len(final_df)):
        
        current_row = final_df.iloc[index, :]
        current_row_distances = np.array([elem for count, elem in enumerate(current_row) if count%2 == 0 and count > 1])
        current_row_voted= np.array([elem for count, elem in enumerate(current_row) if count%2 != 0 and count > 1])

        # Converting everything below X miles to nans, AKA filtering them out
        current_row_distances = np.where(current_row_distances >= max_distance, np.nan, current_row_distances).tolist() 

        # Index of the k closest
        index_of_k_closest = np.argsort(current_row_distances)[:k_neighbors]

        ncid = final_df.iloc[index, :]['ncid']
        ncid_voted = final_df.iloc[index, :]['voted'] 

        # Generating the row that will be added
        row_to_add = [[ncid, ncid_voted]]
        for elem in index_of_k_closest:
            current_list = []
            if np.isnan(current_row_distances[elem]):
                current_list.append(np.nan)
                current_list.append(np.nan)
            else:
                current_list.append(current_row_distances[elem])
                current_list.append(current_row_voted[elem])
            row_to_add.append(current_list)

        # Flattening the list 
        row_to_add = [elem for sublist in row_to_add for elem in sublist]
        k_nearest_df.loc[index] = row_to_add
        
    return k_nearest_df



# Running it
max_distance_input = click.prompt('Maximum distance (in miles)')
k_neighbors_input = click.prompt('K neighbors')

filtered_df = control_set(max_distance=int(max_distance_input), k_neighbors=int(k_neighbors_input))




# Some descriptive stats 
print("Mean closest neighbor1 distance: " + str(filtered_df['Neighbor1Distance'].mean()))
print("Median closest neighbor1 distance: " + str(filtered_df['Neighbor1Distance'].median()))
print("Mean closest neighbor20 distance: " + str(filtered_df['Neighbor20Distance'].mean()))
print("Median closest neighbor20 distance: " + str(filtered_df['Neighbor20Distance'].median()))



# # Exporting full csv
# full_maxdistance2_kneighbors20 = filtered_df.to_csv('full_maxdistance2_kneighbors20.csv')

# # Exporting high-level summary csv
# high_level_summary = filtered_df.describe()
# two_decimal_longitude_maxdistance2_kneighbors20 = high_level_summary.to_csv('two_decimal_longitude_maxdistance2_kneighbors20.csv')

