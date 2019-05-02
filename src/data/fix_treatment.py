import pandas as pd
import click

def main(filepath, outfile):
    # Load in flawed treatment file, 2012 voter registration
    df = pd.read_csv(filepath)
    idx = df.index[df['ncid'].isin(['CW600805', 'CW659359', 'CW604162'])]
    df.update(pd.DataFrame({'poll_changed': [0, 0, 0]}, index=idx))
    df = df.drop(['Unnamed: 0', 'Unnamed: 0.1'], axis=1)
    df = df.dropna(subset=['latitude', 'longitude'])
    df.to_csv('../../data/processed/' + outfile, index=False)

if __name__ == '__main__':    
    filepath = click.prompt('Filepath to full_df.csv',
                                   default='../../data/interim/full_df.csv',
                                   show_default=True,
                                   type=click.Path(exists=True))
    outfile = click.prompt('Name to write new file to',
                           default='fixed_full_df.csv',
                           show_default=True,
                           type=str)
    main(filepath, outfile)