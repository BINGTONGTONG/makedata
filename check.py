# coding: UTF-8
# input script according to definition of "run" interface
import pyspark
from trailer import logger
from pyspark import SparkContext
from pyspark.sql import SQLContext


sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)


# raw tables
prefix = "hdfs:///baoxinqi/anti-money/data_group1"
trx_data = context.read.parquet(prefix + "/trx")
str_data = context.read.parquet(prefix + "/str")
corporate = context.read.parquet(prefix + "/corporate")
account = context.read.parquet(prefix + "/account")
retail = context.read.parquet(prefix + "/retail")

# trx filter
str_data.createTempView("str")
trx_data.createTempView("trx_all")
trx_data_filtered = context.sql("""
    with tbl_all_id as 
    (
        select
            trx_all.cust_id,
            trx_all.target_cust_id
        from str
        join trx_all
        on str.trx_id = trx_all.trx_id
    ), tbl_all_id_dis as
    (
        select
            distinct cust_id as c_id
        from tbl_all_id 

        union 

        select
            distinct target_cust_id as c_id
        from tbl_all_id 
    )

    select
        trx_all.*
    from tbl_all_id_dis
    join trx_all
    on tbl_all_id_dis.c_id = trx_all.cust_id
    """)

trx_join = trx_data_filtered.join(retail, ["cust_id"], "left") \
                            .join(corporate, ["cust_id"], "left") \
                            .join(account, ["account_id"], "left")
print("relevant trx: " + str(trx_join.count))
print("str table: " + str(str_data.count))
print("str distinct: " + str(str_data.select("inst_id").distinct().count()))

