Performance summary
===================

Script: ``summary.py``. Run::
   
   $ python summary.py -h
   usage: summary.py [-h] [-i INDEX] [-q QUANTILE] [-n NUMBER] [-l]
                     [-f {daily,weekly,monthly}] [-b {A,Q,M,W}]
                     [-g {ir,turnover,returns,all}] [-c COST]
                     alpha

   positional arguments:
     alpha                 Alpha file

   optional arguments:
     -h, --help            show this help message and exit
     -i INDEX, --index INDEX
                           Index name; set this only when option --longonly is 
                           turned on
     -q QUANTILE, --quantile QUANTILE
                           When --longonly is turned on, this can be negative to
                           choose the bottom quantile; when not, this sets a
                           threshold to choose tail quantiles
     -n NUMBER, --number NUMBER
                           When --longonly is turned on, this can be negative to
                           choose the bottom; when not, this sets a threshold to
                           choose tail
     -l, --longonly        Whether to test this alpha as a longonly holding
     -f {daily,weekly,monthly}, --freq {daily,weekly,monthly}
                           Which frequency of statistics to be presented? For
                           example, "weekly" means to show IR(5) besides IR(1) if
                           --group is set to be "ir" or "all"
     -b {A,Q,M,W}, --by {A,Q,M,W}
                           Summary period
     -g {ir,turnover,returns,all}, --group {ir,turnover,returns,all}
                           Performance metrics group
     -c COST, --cost COST  Linear trading cost
