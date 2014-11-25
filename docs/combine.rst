Alpha combiner
==============

Script: ``combiner.py``. Run::

   $ python combiner.py -h
   usage: combiner.py [-h] [--periods PERIODS] [--quantile QUANTILE] [--debug_on]
                      [--is_start IS_START] [--is_end IS_END]
                      [--os_start OS_START] [--os_end OS_END] [--dir DIR]
                      [--file FILE] [--dump DUMP]

   optional arguments:
     -h, --help           show this help message and exit
     --periods PERIODS    Days of returns
     --quantile QUANTILE  Return quantiles used as dependent variables in
                          regression
     --debug_on           Whether display debug log message
     --is_start IS_START  IS startdate
     --is_end IS_END      IS enddate
     --os_start OS_START  OS startdate
     --os_end OS_END      OS enddate
     --dir DIR            Input directory, each file contained is assumed to be
                          an alpha file
     --file FILE          Input file, each row in the format: name
                          path_to_a_csv_file
     --dump DUMP          The output file name
