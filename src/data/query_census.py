# This file downloads specific data from the US census, then writes it to .tsv
import pandas as pd 
import click
from census import Census

def query_census(census, col_id, col_name=None, fips='37'):
    '''
    Calls the census API to get a given column, output it to a pandas Dataframe
    :census: Initialized Census object for querying the US census API
    :col_id: ID of the ACS5 census metric (e.g. B08303_001E = travel time to work)
    :col_name: Name to assign to the column id
    :fips: str, Code for the state of interest (North Carolina is '37')
    '''
    if col_name == None:
        col_name = col_id
    data = census.acs5.get(('NAME', col_id),
                    {'for': 'tract:*', 'in': 'state:{}'.format(fips)})
    return pd.DataFrame(data).rename({col_id: col_name}, axis=1)

def census_dataframe(census, col_dict, fips='37'):
    '''
    Build a Dataframe of columns queried from the census by inner joining each metric on UID (NAME)
    :census: Initialized Census object for querying the US census API
    :col_dict: Dictionary of {'column_id': 'Name you choose for the column'}
    :fips: str, Code for the state of interest (North Carolina is '37')
    '''
    df = None
    for key in col_dict:
        q = query_census(census, col_id=col_dict[key], col_name=key, fips=fips)
        if df is None:
            df = q
        else: 
            q = q[['NAME', key]]
            df = df.merge(q, how='inner', on='NAME')
    return df

if __name__ == '__main__':

    # Some values need to be hard-coded
    api_key = 'bf07f59168c73d5c7d1d32171dfa455345f8c7c0'
    fips_codes = {'NC': '37', 'FL': '12', 'OH': '39'}
    columns = {'population': 'B02001_001E',
               'white_population': 'B02001_002E',
               'travel_time': 'B08303_001E',
               'hh_inc': 'B19001_001E',
               'med_hh_inc': 'B19013_001E',
               'agg_hh_inc': 'B19025_001E'}


    c = Census(api_key)

    output_directory = click.prompt('Output directory to write processed data',
                                    default='../../data/raw/',
                                    show_default=True,
                                    type=click.Path(exists=True))

    state = click.prompt('Which state are you querying for?',
                         default='NC',
                         show_default=True,
                         type=str)


    df = census_dataframe(c,col_dict=columns, fips=fips_codes[state])
    df.to_csv(output_directory + 'census_{}.tsv'.format(state), sep='\t')
    

    