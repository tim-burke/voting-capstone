import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import matplotlib.pyplot as plt
import seaborn as sns
import click

def is_absentee(vm):
    '''Takes voting method, recodes to 1 or 0'''
    if vm is np.nan:
        return 0
    elif vm in {'ABSENTEE ONESTOP', 'ABSENTEE BY MAIL', 'ABSENTEE CURBSIDE', 'ABSENTEE'}:
        return 1
    else: 
        return 0

def in_person(vm):
    '''Takes voting method, recodes to 1 or 0'''
    if vm is np.nan:
        return 0
    elif vm in {'ABSENTEE ONESTOP', 'ABSENTEE BY MAIL', 'ABSENTEE CURBSIDE', 'ABSENTEE'}:
        return 0
    else: 
        return 1

def load_data(filepath):
    df = pd.read_csv(filepath, sep='\t')
    df['closer'] = df['delta_dist'].apply(lambda x: 1 if x < 0 else 0)
    df['farther'] = df['delta_dist'].apply(lambda x: 1 if x > 0 else 0)
    df['much_farther'] = df['delta_dist'].apply(lambda x: 1 if x > 1 else 0)
    df['much_closer'] = df['delta_dist'].apply(lambda x: 1 if x < -1 else 0)
    df['white'] = df['race_code'].apply(lambda x: 1 if x == 'W' else 0)
    df['absentee'] = df['voting_method'].apply(is_absentee)
    df['in_person'] = df['voting_method'].apply(in_person)
    return df

def poll_dist_kdeplot(df, outpath):
    cal_colors = ['#FDB515', '#003262']
    sns.set_palette(palette=cal_colors)
    dists = df[df['delta_dist'] != 0]['delta_dist'].values
    fig, ax = plt.subplots()
    ax.set_title('Distribution of Non-Zero Polling Distance Changes')
    ax.set_xlabel('Change in distance to polling place')
    sns.distplot(dists, ax=ax);
    plt.savefig(outpath + 'distance_kdeplot.png', format='png')
    print('Plotted distance KDE plot')

def turnout_barplots(df, outpath):
    voting = df.groupby('poll_changed')['voted'].mean().values[::-1]
    absentee = df.groupby('poll_changed')['absentee'].mean().values[::-1]
    in_person = df.groupby('poll_changed')['in_person'].mean().values[::-1]
    
    fig, axes = plt.subplots(nrows=3, ncols=1, sharex=True)
    cal_colors = ['#FDB515', '#003262']
    sns.set_palette(palette=cal_colors)

    # Overall Plot
    ax1 = axes[0]
    ax1.set_title('Overall Voter Turnout')
    ax1.set_xlim([0, 0.9])
    vals = ax1.get_xticks()
    ax1.set_xticklabels(['{:,.1%}'.format(x) for x in vals])
    sns.barplot(y=['Poll Changed', 'No Change'], x=voting, ax=ax1);

    # Absentee
    ax2 = axes[1]
    ax2.set_title('Absentee Turnout')
    ax2.set_xlim([0, 0.9])
    vals = ax2.get_xticks()
    ax2.set_xticklabels(['{:,.1%}'.format(x) for x in vals])
    sns.barplot(y=['Poll Changed', 'No Change'], x=absentee, ax=ax2);

    # In Person
    ax3 = axes[2]
    ax3.set_title('In-Person Turnout')
    ax3.set_xlim([0, 0.9])
    vals = ax3.get_xticks()
    ax3.set_xticklabels(['{:,.0%}'.format(x) for x in vals])
    sns.barplot(y=['Poll Changed', 'No Change'], x=in_person, ax=ax3);

    plt.tight_layout()
    plt.savefig(outpath + 'turnout_barplot.png', format='png')
    print('Plotted voter turnout bar plots')

def race_stacked_barplot(df, outpath):
    
    # Divide df into treatment and control groups
    t = df[df['poll_changed'] == 1]
    c = df[df['poll_changed'] == 0]

    # Get percents of white/nonwhite in each group
    t_pcts = t.groupby('white')['ncid'].count()
    t_wht, t_nw = ((t_pcts / t.shape[0]) * 100).values
    c_pcts = c.groupby('white')['ncid'].count()
    c_wht, c_nw = ((c_pcts / c.shape[0]) * 100).values

    # Create stacked bars
    fig, ax = plt.subplots()
    bar_width = .8
    bar_idxs = [0, 1]
    ax.bar(bar_idxs, 
           [c_wht, t_wht],
           label='Non-White',
           color='#FDB515',
           width=bar_width);
    ax.bar(bar_idxs, 
           [c_nw, t_nw],
           bottom=[c_wht, t_wht],
           label='White',
           color='#003262',
           width=bar_width);
    ax.set_title('Racial Makeup - Treatment and Control');
    ax.set_xticks([0, 1]);
    ax.set_xticklabels(['Control', 'Treatment']);
    vals = ax.get_yticks();
    ax.set_yticklabels([str(int(x)) + '%' for x in vals]);
    ax.legend(bbox_to_anchor=(1,1));

    plt.savefig(outpath + 'race_barplot.png', format='png')
    print('Plotted voter race stacked bar plots')

def main(filepath, outpath):
    '''
    Takes path to finalized data, writes visualizations to given outpath
    '''
    plt.style.use('fivethirtyeight')
    df = load_data(filepath)
    poll_dist_kdeplot(df, outpath)
    turnout_barplots(df, outpath)
    race_stacked_barplot(df, outpath)

if __name__ == '__main__':
    filepath = click.prompt('Filepath to final geocoded dataset',
                                   default='../../data/processed/finalized_data.tsv',
                                   show_default=True,
                                   type=click.Path(exists=True))
    outpath = click.prompt('Filepath to save figures',
                                   default='../../reports/figures/',
                                   show_default=True,
                                   type=click.Path(exists=True))
    main(filepath, outpath)