# coding: UTF-8
# input script according to definition of "run" interface
import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
import random
import uuid
import datetime
import time

import os
print(os.path())


# cons.py
CITY_LIST = map(str, range(100))
ORG_LIST = map(str, range(1000, 2000))
TYPE_LIST = 'abcdefghijklmnopqrstuvwxyz'
COUNTRY_LIST = map(str, range(200))
FLAG_LIST = map(str, range(2))
BRANCH_LIST = map(str, range(1000))
BRANCH_OFFICE_LIST = map(str, range(1000))
NUM_LIST = map(str, range(10))
CURR_LIST = ['0', '1', '2']


num_vol = 10000
num_rel_trx = num_vol * 20
num_rel_cust = num_vol
num_rel_account = num_rel_cust * 2

num_account = 15 * 10000 * 10000  # 账户数量
num_trx = 10 * 10000 * 10000  # 10亿交易
num_company = 300 * 10000
num_persion = 3 * 10000 * 10000
num_cust = num_company + num_persion


# util.py
def gen_evt_id(prefix):
    return prefix + '_' + str(uuid.uuid4())

def gen_rand_time_wrapper(min_date, max_date, format_ori='%Y-%m-%d', format_aft='%Y-%m-%d %H:%M:%S'):
    date_max = str2datetime(min_date, format_ori)
    date_min = str2datetime(max_date, format_ori)
    diff_time = date_max - date_min
    return gen_rand_time(diff_time, date_min, format_aft)

def gen_rand_time(diff_time, date_min, format_str='%Y-%m-%d %H:%M:%S'):
    seconds_delta = int(random.random() * diff_time.total_seconds())
    timestamp = date_min + datetime.timedelta(seconds=seconds_delta)
    return timestamp.strftime(format_str)

def str2datetime(day_str, format_str='%Y-%m-%d'):
    date_str = time.strptime(day_str, format_str)
    return datetime.datetime(date_str[0], date_str[1], date_str[2])


# logger.py
import logging

logging.basicConfig(
        format="%(levelname)s %(asctime)s [%(filename)s +%(lineno)s %(funcName)s] %(message)s",
        level=logging.WARNING)

logger = logging.getLogger("qinyikun@4paradigm.com")
logger.setLevel(logging.DEBUG)


# ========
# 可疑卷宗
# ========
def gen_tbl_fraud(id_vol_rdd, id_trx_rdd):
    min_date = '2016-01-01'
    max_date = '2017-03-01'
    schema = ['ins_date', 'label', 'rule_id', 'p_flag', 'trx_id', 'ins_id']

    def __mygen(vol_id, do_sample):
        result = []
        if not do_sample:
            sample_cnt = 1
        else:
            sample_cnt = random.randint(20, 50)
        label = '1' if random.random() < 0.4 else '0'
        v_date = gen_rand_time_wrapper(min_date, max_date, format_aft='%Y-%m-%d')
        for j in range(sample_cnt):
            # trx_id = random.choice(id_trx_list)
            trx_id_int = random.randint(0, num_trx)
            rule = 'rule_' + random.choice(TYPE_LIST)
            p_flag = random.choice(FLAG_LIST)
            result.append( (trx_id_int, [v_date, label, rule, p_flag, vol_id]) )
        return result

    id_vol_rdd.persist()
    mygen_rdd_base = id_vol_rdd.flatMap(lambda x: __mygen(x, False))
    mygen_rdd_extra = id_vol_rdd.flatMap(lambda x: __mygen(x, True)).sample(False, (num_rel_trx - num_vol) * 1.0 / num_rel_trx, 131)
    mygen_rdd = mygen_rdd_base.union(mygen_rdd_extra)

    return mygen_rdd.join(id_trx_rdd).map(lambda x: (x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][1], x[1][0][4])), schema


# ========
# 交易表
# ========
# trx_id_rdd 需要去重
def gen_tbl_trx_rdd(trx_id_rdd, id_cust_list, id_account_list):
    max_trx_amt = 10 ** 8
    min_date = '2016-01-01'
    max_date = '2017-03-01'
    trx_id_rdd = trx_id_rdd.map(lambda x: (x, \
                                random.choice(id_cust_list), \
                                gen_rand_time_wrapper(min_date, max_date, format_aft='%Y-%m-%d %H:%M:%S'), \
                                'curr_' + random.choice(CURR_LIST), \
                                random.choice(FLAG_LIST), \
                                random.choice(FLAG_LIST), \
                                str(max_trx_amt * random.random()), \
                                str(max_trx_amt * random.random()), \
                                random.choice(id_account_list), \
                                random.choice(id_cust_list), \
                                random.choice(id_account_list), \
                                'trx_cd_' + random.choice(TYPE_LIST), \
                                'tgt_' + random.choice(TYPE_LIST), \
                                random.choice(FLAG_LIST), \
                                random.choice(FLAG_LIST)))

    trx_id_rdd = trx_id_rdd.map(lambda x:(x[0],x[1],x[3],x[4],x[5], \
                                          x[6],x[7],x[8],x[9],x[10],x[11], \
                                          x[12],x[13],x[14], \
                                          x[2][:10], \
                                          x[2][11:]
                                ))

    schema = ['trx_id', 'cust_id', 'trx_curr', 'flag_cash', 'flag_crdr', 'trx_amt', 'balance_amt', 'account_id', 'target_cust_id',
              'target_account_id', 'trx_code', 'target_type', 'target_third_pay', 'target_offshore', 'trx_date', 'trx_time']
    return trx_id_rdd, schema


# =============
# 零售客户信息表
# =============

def gen_tbl_cust_retail_info(id_cust_rdd):
    def __gen(cust_id):
        doc_type = 'doc_' + random.choice(TYPE_LIST)
        doc_country = 'country_' + random.choice(COUNTRY_LIST)
        age = str(random.randint(20, 90))
        nation = 'nation_' + random.choice(TYPE_LIST)
        job = 'job_' + random.choice(TYPE_LIST)
        edu = 'edu_' + random.choice(TYPE_LIST)
        marriage = random.choice(FLAG_LIST)
        return [cust_id, doc_type, doc_country, nation, job, edu, marriage, age]
    result = id_cust_rdd.map(lambda x: __gen(x))
    schema = ['cust_id', 'doc_type', 'doc_country', 'nation', 'job', 'edu', 'marriage', 'age']
    return result, schema


# ============
# 对公客户信息表
# ============
def gen_tbl_company_info(id_company_rdd):
    max_amt = 10 ** 8
    def __gen(company_id):
        ind_type = 'ind_' + random.choice(TYPE_LIST)
        own_type = 'own_' + random.choice(TYPE_LIST)
        reg_curr = 'curr_' + random.choice(CURR_LIST)
        reg_amt = str(max_amt * random.random())
        age = str(random.randint(20, 90))
        return [company_id, ind_type, own_type, reg_curr, reg_amt, age]
    result = id_company_rdd.map(lambda x: __gen(x))
    schema = ['cust_id', 'ind_type', 'own_type', 'reg_curr', 'reg_amt', 'legal_age']
    return result, schema


# ====================
# 户口信息，非信用卡片表
# ====================
def gen_tbl_account(id_account_rdd):
    min_date = '1998-01-01'
    max_date = '2017-03-01'
    def __gen(account_id):
        account_type = 'acc_' + random.choice(TYPE_LIST)
        offshore = random.choice(FLAG_LIST)
        open_date = gen_rand_time_wrapper(min_date, max_date, format_aft='%Y-%m-%d')
        close_date = gen_rand_time_wrapper(min_date, max_date, format_aft='%Y-%m-%d')
        return [account_id, account_type, offshore, open_date, close_date]
    result = id_account_rdd.map(lambda x: __gen(x))
    schema = ['account_id', 'account_type', 'offshore', 'open_date', 'close_date']
    return result, schema


if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = SQLContext(sc)
    random.seed(131)
    prefix = "hdfs:///baoxinqi/anti-money/data_group1/"

    logger.info('generating id_account_list')
    id_account_rdd = sc.parallelize(xrange(num_account), 1024).map(lambda x: gen_evt_id('acc'))
    id_account_rdd.persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)
    logger.info('saving tbl_account')
    tbl_account, schema_account = gen_tbl_account(id_account_rdd)
    tbl_account_df = sqlContext.createDataFrame(tbl_account, schema_account)
    tbl_account_df.write.parquet(prefix + "/account")
    id_account_list = id_account_rdd.take(num_rel_account)
    id_account_rdd.unpersist()

    logger.info('generating id_company_list')
    id_company_rdd = sc.parallelize(xrange(num_company), 1024).map(lambda x: gen_evt_id('cust_company_'))
    id_company_rdd.persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)
    logger.info('saving tbl_corporate')
    tbl_corporate, schema_corporate = gen_tbl_company_info(id_company_rdd)
    tbl_corporate_df = sqlContext.createDataFrame(tbl_corporate, schema_corporate)
    tbl_corporate_df.write.parquet(prefix + "/corporate")
    id_company_list = id_company_rdd.take(int(num_rel_cust * 0.3))
    id_company_rdd.unpersist()

    logger.info('generating id_person_list')
    id_person_rdd = sc.parallelize(xrange(num_persion), 1024).map(lambda x: gen_evt_id('cust_persion_'))
    id_person_rdd.persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)
    logger.info('saving tbl_retail')
    tbl_retail, schema_retail = gen_tbl_cust_retail_info(id_person_rdd)
    tbl_retail_df = sqlContext.createDataFrame(tbl_retail, schema_retail)
    tbl_retail_df.write.parquet(prefix + "/retail")
    id_person_list = id_person_rdd.take(int(num_rel_cust * 0.7))
    id_cust_list = id_company_list + id_person_list
    id_person_rdd.unpersist()
    logger.info('generation cust list successed!')


    # generating id_list
    logger.info('generating id_trx_rdd')
    id_trx_rdd_pre = sc.parallelize(xrange(num_trx), 512).map(lambda x: (x, gen_evt_id('trx')))
    id_trx_rdd_pre.persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)
    id_trx_rdd = id_trx_rdd_pre.map(lambda x: x[1])


    logger.info('generating id_vol_list')
    id_vol_rdd = sc.parallelize(xrange(num_vol), 512).map(lambda x: gen_evt_id('vol'))


    logger.info('saving tbl_trx')
    tbl_trx_rdd, schema_trx = gen_tbl_trx_rdd(id_trx_rdd, id_cust_list, id_account_list)
    tbl_trx_df = sqlContext.createDataFrame(tbl_trx_rdd, schema_trx)
    tbl_trx_df.cache()

    logger.info('saving tbl_fraud')
    tbl_fraud_list, schema_fraud = gen_tbl_fraud(id_vol_rdd, id_trx_rdd_pre)
    tbl_str_df = sqlContext.createDataFrame(tbl_fraud_list, schema_fraud)


    tbl_str_df.write.parquet(prefix + "/str")
    tbl_trx_df.write.parquet(prefix + "/trx")

