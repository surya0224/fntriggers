"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-05-10
File Name: logutils.py

Context Manager for logging all python python.
The wrapper should be added on top of all python functions to be logged.

"""
import time
import logging
import json
from json import JSONEncoder
from jupiter_AI.network_level_params import SYSTEM_DATE
import datetime
import inspect
from bson.json_util import dumps, loads
from bson import ObjectId
import traceback



class MeasureEncoder(JSONEncoder):
    def default(self, o):
        if inspect.isframe(o):
            return {}
        elif isinstance(o, datetime.datetime):
            return o.isoformat()
        # elif isinstance(o, ObjectId):
        #    return {}
        elif hasattr(o, "__dict__"):
            return o.__dict__
        else:
            return JSONEncoder.default(self, o)


class MeasureContextManager(object):
    def __init__(self, funcName, logger, *args, **kwargs):
        self.funcName = funcName
        self.logger = logger
        self.args = args
        self.kwargs = kwargs
        # self.stats = {'funcName': self.funcName, 'args': str(args), 'kwargs': str(kwargs)}
        self.stats = {'funcName': self.funcName}

    def __enter__(self):
        self.init_time = time.time()
        self.logger.info(
            ' Started: {} timestamp: {}'.format(self.funcName, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        time_taken = time.time() - self.init_time
        self.stats['time_taken'] = time_taken
        # delimiter = '> stats :: '
        # delimiter2 = '> args :: '
        # self.logger.info(delimiter + json.dumps(self.stats))
        # self.logger.info(
        #     '{}funcName={}, args={}, kwargs={}'.format(delimiter2, self.funcName, self.args, self.kwargs))
        # for atr in dir(tb):
        #     print atr, tb.atr

        if exc_type is None:
            self.logger.info(' Finished: {} timestamp: {} time_taken: {}'.format(self.funcName,
                                                                                 datetime.datetime.now().strftime(
                                                                                    '%Y-%m-%d %H:%M:%S'),
                                                                                 time_taken))
            self.logger.info(' System date is : ')
            system_date="2020-06-14"
            self.logger.info(system_date)
        else:
            self.logger.info(' Failed: {} timestamp: {} traceback: {}'.format(self.funcName,
                                                                              datetime.datetime.now().strftime(
                                                                                 '%Y-%m-%d %H:%M:%S'),
                                                                              traceback.format_exc(exc_tb)))


def measure(logger):
    def measure_decorator(func):
        def wrapper(*args, **kwargs):
            with MeasureContextManager(func.__name__, logger, *args, **kwargs):
                return func(*args, **kwargs)

        return wrapper

    return measure_decorator
