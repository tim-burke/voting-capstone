import os
import re
import click
outdir = '../data/interim/'
name = 'NC_interim.tsv'
dask_fmt = 'NC-*.tsv'

def main(outdir, name, dask_fmt):
    '''
    Made to combine all CSVs in a directory written by Dask into a single file.
    Assumes each csv contains a header, order irrelevant. 
    outdir: Directory containing CSVs & where to write combined CSV
    name: Name of new combined CSV file
    dask_fmt: format of csv names to be combined
    '''
    dir_tree = os.walk(outdir)
    _, _, filenames = next(dir_tree) # list of filenames in outdir
    dask_re = dask_fmt.replace('*', '\d+')
    filenames = [f for f in filenames if re.fullmatch(dask_re, f)]
    with open(outdir + name, 'w') as outfile:
        # Write first csv in full to outfile
        with open(outdir + filenames[0]) as file:
            for line in file:
                outfile.write(line)
        # Write the rest to outfile w/o headers
        for f in filenames[1:]:
            with open(outdir + f) as file:
                next(file)
                for line in file:
                    outfile.write(line)

if __name__ == '__main__':
    outdir = click.prompt('Output directory containing CSVs to combine',
                          default='../../data/interim/',
                          show_default=True,
                          type=click.Path(exists=True)) 
    name = click.prompt('Name for the combined CSV file', 
                        default='NC_interim.tsv', 
                        show_default=True, 
                        type=str)
    dask_fmt = click.prompt('Format of the CSV filenames', 
                            default='NC-*.tsv', 
                            show_default=True, 
                            type=str)
    main(outdir, name, dask_fmt)