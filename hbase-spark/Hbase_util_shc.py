# -*- coding: UTF-8 -*-
# add by Kylin

from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import split ,col
import sys
from datetime import datetime


def readDataFrame(spark):
    """
        this function based on hortonworks shc library , 
        it implements a datasource for us , and can integrate with spark sql 
        https://hbase.apache.org/book.html#_sparksql_dataframes
    """
    catalog = ''.join("""{
        "table":{"namespace":"nm", "name":"your_hbase_table"},
        "rowkey":"key",
        "columns":{
            "user_id":{"cf":"rowkey", "col":"key", "type":"string"},
            "user_level":{"cf":"cf", "col":"user_level", "type":"string"},
            "gender":{"cf":"cf", "col":"gender", "type":"string"},
            "click_num":{"cf":"cf", "col":"click_num", "type":"string"},
            "click_rate":{"cf":"cf", "col":"click_rate", "type":"string"}
        }
    }""".split())
    data_source_format = 'org.apache.spark.sql.execution.datasources.hbase'
    df = spark.read.options(catalog=catalog).format(data_source_format).load()
    return df

def writeDataFrame(df):
    """
        this function aimed to write a dataframe to hbase 
        namespace you can choose default
    """
    catalog = ''.join("""{
        "table":{"namespace":"test", "name":"test"},
        "rowkey":"key",
        "columns":{
            "rkey":{"cf":"rowkey", "col":"key", "type":"string"},
            "colname1":{"cf":"cf", "col":"colname1", "type":"string"},
            "colname2":{"cf":"cf", "col":"colname2", "type":"string"},
            "colname3":{"cf":"cf", "col":"colname3", "type":"string"},
            "colname4":{"cf":"cf", "col":"colname4", "type":"string"}
        }
    }""".split())
    df.write.options(catalog =  catalog ).format('org.apache.spark.sql.execution.datasources.hbase').save()
    return 


# df = sqlContext.read.format('org.apache.hadoop.hbase.spark') \
#     .option('hbase.table','books') \
#     .option('hbase.columns.mapping', \
#             'title STRING :key, \
#             author STRING info:author, \
#             year STRING info:year, \
#             views STRING analytics:views') \
#     .option('hbase.use.hbase.context', False) \
#     .option('hbase.config.resources', 'file:///etc/hbase/conf/hbase-site.xml') \
#     .option('hbase-push.down.column.filter', False) \
#     .load()

# df.show()

# test 
# from pyspark.sql.types import Row
# data  = [ Row( rkey = "rk{}".format(i) , colname1 = "c{}".format(i) , colname2 = "c{}".format(i+1) ,  colname3 = "c{}".format(i+2) , colname4 = "c{}".format(i+3)  ) for i in range(10) ]
# df = sc.parallelize(data).toDF()
# df.write.options(catalog=catalog).format('org.apache.spark.sql.execution.datasources.hbase').save()

# hbase(main):001:0> scan 'test:test'
# ROW                                                          COLUMN+CELL
#  rk0                                                         column=cf:colname1, timestamp=1559805107212, value=c0
#  rk0                                                         column=cf:colname2, timestamp=1559805107212, value=c1
#  rk0                                                         column=cf:colname3, timestamp=1559805107212, value=c2
#  rk0                                                         column=cf:colname4, timestamp=1559805107212, value=c3
#  rk1                                                         column=cf:colname1, timestamp=1559805107212, value=c1
#  rk1                                                         column=cf:colname2, timestamp=1559805107212, value=c2
#  rk1                                                         column=cf:colname3, timestamp=1559805107212, value=c3
#  rk1                                                         column=cf:colname4, timestamp=1559805107212, value=c4
#  rk2                                                         column=cf:colname1, timestamp=1559805107212, value=c2
#  rk2                                                         column=cf:colname2, timestamp=1559805107212, value=c3
#  rk2                                                         column=cf:colname3, timestamp=1559805107212, value=c4
#  rk2                                                         column=cf:colname4, timestamp=1559805107212, value=c5
#  rk3                                                         column=cf:colname1, timestamp=1559805107212, value=c3
#  rk3                                                         column=cf:colname2, timestamp=1559805107212, value=c4
#  rk3                                                         column=cf:colname3, timestamp=1559805107212, value=c5
#  rk3                                                         column=cf:colname4, timestamp=1559805107212, value=c6
#  rk4                                                         column=cf:colname1, timestamp=1559805107212, value=c4
#  rk4                                                         column=cf:colname2, timestamp=1559805107212, value=c5
#  rk4                                                         column=cf:colname3, timestamp=1559805107212, value=c6
#  rk4                                                         column=cf:colname4, timestamp=1559805107212, value=c7
#  rk5                                                         column=cf:colname1, timestamp=1559805107214, value=c5
#  rk5                                                         column=cf:colname2, timestamp=1559805107214, value=c6
#  rk5                                                         column=cf:colname3, timestamp=1559805107214, value=c7
#  rk5                                                         column=cf:colname4, timestamp=1559805107214, value=c8
#  rk6                                                         column=cf:colname1, timestamp=1559805107214, value=c6
#  rk6                                                         column=cf:colname2, timestamp=1559805107214, value=c7
#  rk6                                                         column=cf:colname3, timestamp=1559805107214, value=c8
#  rk6                                                         column=cf:colname4, timestamp=1559805107214, value=c9
#  rk7                                                         column=cf:colname1, timestamp=1559805107214, value=c7
#  rk7                                                         column=cf:colname2, timestamp=1559805107214, value=c8
#  rk7                                                         column=cf:colname3, timestamp=1559805107214, value=c9
#  rk7                                                         column=cf:colname4, timestamp=1559805107214, value=c10
#  rk8                                                         column=cf:colname1, timestamp=1559805107214, value=c8
#  rk8                                                         column=cf:colname2, timestamp=1559805107214, value=c9
#  rk8                                                         column=cf:colname3, timestamp=1559805107214, value=c10
#  rk8                                                         column=cf:colname4, timestamp=1559805107214, value=c11
#  rk9                                                         column=cf:colname1, timestamp=1559805107214, value=c9
#  rk9                                                         column=cf:colname2, timestamp=1559805107214, value=c10
#  rk9                                                         column=cf:colname3, timestamp=1559805107214, value=c11
#  rk9                                                         column=cf:colname4, timestamp=1559805107214, value=c12
# 10 row(s) in 0.2470 seconds
