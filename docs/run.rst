Run script
==========

Script:: ``run.py``. Run::

   $ python run.py -h
   usage: run.py [-h] [-s START] [-e END] [--param PARAM] [--file FILE]
                 [--csvdir CSVDIR] [--hdf HDF]
                 alpha
                               
   positional arguments:
     alpha                 Alpha .py file
    
   optional arguments:
     -h, --help            show this help message and exit
     -s START, --start START
                           Simulation starting date
     -e END, --end END     Simulation ending date
     --param PARAM         A quoted string to supply keyword parameters; for
                           example: --param='p1=p1v1,p1v2;p2=p2v1,p2v2.p3v3'
     --file FILE           A file containing configuration parameters(A .py file
                           is recommended)
     --csvdir CSVDIR       Diretory to dump generated DataFrames
     --hdf HDF             HDF5 file name to save generated DataFrames
