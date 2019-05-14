# coding: UTF-8

import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
import random
import uuid
import datetime
import time


city_list = map(str,range(100))

num_vol = 10000



def gen_evt_id(prefix):
    return prefix + '_' + str(uuid.uuid4())
