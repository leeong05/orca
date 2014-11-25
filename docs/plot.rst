Plotting
========

Script: ``plotter.py``. Run::

   $ python plotter.py -h
   usage: plotter.py [-h] [-i INDEX] [-q QUANTILE] [-n NUMBER] [-l]
                     [-p PLOT [PLOT ...]] [-b {A,Q,M,W}] [-c COST] [--plot_index]
                     [--ma MA] [--periods PERIODS] [--pdf] [-s START] [-e END]
                     alpha

   positional arguments:
     alpha                 Alpha file

   optional arguments:
     -h, --help            show this help message and exit
     -i INDEX, --index INDEX
                           Name of the index, for example: HS300. Set this only
                           when --longonly is turned on
    -q QUANTILE, --quantile QUANTILE
                           When --longonly is turned on, this can be negative to
                           choose the bottom quantile; when not, this sets a
                           threshold to choose tail quantile
   -n NUMBER, --number NUMBER
                           When --longonly is turned on, this can be negative to
                           choose the bottom; when not, this sets a threshold to
                           choose tail
   -l, --longonly        Whether to test this alpha as a longonly holding
   -p PLOT [PLOT ...], --plot PLOT [PLOT ...]
                         What to plot? Could by any combination of ("pnl",
                         "returns", "ic", "turnover", "ac")
   -b {A,Q,M,W}, --by {A,Q,M,W}
                         Summary period
   -c COST, --cost COST  Linear trading cost
   --plot_index          Add index data for "pnl"/"returns" plot
   --ma MA               For "ic"/"ac"/"turnover" plot, use simple moving
                         average to smooth
   --periods PERIODS     Periods used in calculation of IC and AC
   --pdf                 Whether to save plots in a PDF file
   -s START, --start START
                         Starting date
   -e END, --end END     Ending date

Script: ``qplotter.py``. Run::
   
   $ python qplotter.py -h
   usage: qplotter.py [-h] -q QUANTILE [-p PLOT [PLOT ...]] [-b {A,Q,M,W}]
                      [--pdf] [-s START] [-e END]
                      alpha

   positional arguments:
     alpha                 Alpha file

   optional arguments:
     -h, --help            show this help message and exit
     -q QUANTILE, --quantile QUANTILE
                           Number of quantiles
     -p PLOT [PLOT ...], --plot PLOT [PLOT ...]
                           What to plot? Could by any combination of ("pnl",
                           "returns")
     -b {A,Q,M,W}, --by {A,Q,M,W}
                           Summary period
     --pdf                 Whether to save plots in a PDF file
     -s START, --start START
                           Starting date
     -e END, --end END     Ending date
