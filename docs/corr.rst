Correlation report
==================

Script: ``correlation.py``. Run::

   $ python correlation.py -h
   usage: correlation.py [-h] [-q QUANTILE] [--db] [-m {ic,returns,both}]
                         [--dir DIR] [--file FILE] [--days DAYS]
                         [alpha [alpha ...]]

   positional arguments:
     alpha                 Alpha file

   optional arguments:
     -h, --help            show this help message and exit
     -q QUANTILE, --quantile QUANTILE
                           Sets threshold for tail quantiles to calculate returns
     --db                  Check correlation with alphas in alphadb
     -m {ic,returns,both}, --method {ic,returns,both}
                           What type of correlations is of interest?
     --dir DIR             Input directory, each file contained is assumed to be
                           an alpha file
     --file FILE           Input file, each row in the format: name
                           path_to_a_csv_file
     --days DAYS           How many points to be included in correlation
                           calculation
