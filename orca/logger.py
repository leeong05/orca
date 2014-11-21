"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import logging
from threading import Lock

init_lock = Lock()
root_logger_initialized = False

log_format = '%(asctime)s %(name)s [%(levelname)s] %(message)s'
level = logging.INFO
file_log = None # file name
console_log = True

def init_handler(handler):
    handler.setFormatter(logging.Formatter(log_format))

def init_logger(logger):
    logger.setLevel(level)

    if file_log is not None:
        file_handler = logging.FileHandler(file_log)
        init_handler(file_handler)
        logger.addHandler(file_handler)

    if console_log:
        console_handler = logging.StreamHandler()
        init_handler(console_handler)
        logger.addHandler(console_handler)

def initialize():
    global root_logger_initialized
    with init_lock:
        if not root_logger_initialized:
            init_logger(logging.getLogger())
            root_logger_initialized = True

def get_logger(name=None):
    initialize()
    return logging.getLogger(name)
