voting_capstone
==============================

Measuring the effect of polling place closures on voter turnout

Project Organization
------------
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── results        <- CSV data of results from different model specifications.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. 
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- PDF version of final reports
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── src                <- Source code for use in this project.
        ├── __init__.py    <- Makes src a Python module
        │
        ├── data           <- Scripts for data cleaning and processing.
        │   │                 
        │   ├── cleaning_functions.py     <- Functions used in cleaning and formatting raw NC voter data. 
        │   └── clean_data.py             <- Transforms raw data into intermediate data that can be geocoded. Writes to CSVs. 
        │   └── combine_CSVs.py           <- Script for combining Dask CSVs into a single file
        │   └── here_geocode.py           <- Geocode CSVs of data written from Dask into latitude/longitude coords using Here API.
        │   └── find_poll_distance.py     <- Takes geocoded data, adds distance from each voter to their polling place.
        │   └── query_census.py           <- Script for getting some descriptive stats on NC from the census. Deprecated. 
        │
        ├── models         <- Scripts for running our model over the finalized data set
        │   │                 
        │   ├── nearest_neighbors.py      <- Takes geocoded data, finds each voter's neighbors within given caliper width
        │   └── make_blocked_df.py        <- Takes geocoded data, makes CSV formatted for running our regression model
        │
        └── visualization  <- Scripts to create exploratory and results oriented visualizations
            └── visualize.py
--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
