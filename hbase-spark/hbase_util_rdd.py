# -*- coding: UTF-8 -*-
# add by Kylin

from pyspark.sql import SparkSession
import json 
from pyspark.sql.types import StructType,StructField,StringType,Row

def flatRow(row,key_col,column_family,column_names):
    """
        this function flat a row to a batch of cols in the hbase put format 
        parameters:
            row -> row is a line from dataframe 
            key_col -> specify rowkey column 
            column_family -> specify column family 
            column_names -> specify all the column names 
    """
    rowkey = str(row[key_col])
    result = []
    for col in column_names:
        one_col = []
        if col == key_col:
            pass
        else:
            if row[col] is None :
                pass
            else :
                one_col.append(rowkey)
                one_col.append(column_family)
                one_col.append(col)
                one_col.append(str(row[col]))
        if len(one_col) > 0 :
            result.append((rowkey,one_col))
    return result
    
zk_hosts = 'zknode1:2181,zknode2:2181,zknode3:2181,zknode4:2181,zknode5:2181'
zk_parent = '/hbase'
def writeDataFrameToHbase(df,zk_hosts,zk_parent,hbase_table,rowkey_col,namespace='nm',column_family='cf'):
    """
        this function flat a dataframe columns ,set the rowkey col  and write this data to the hbase 
        parameters :
            df -> the dataframe you want to write to hbase 
            hbase_table -> which table do you want to write to 
            namespace -> if you have namespace you should specify it 
            column_family -> specify a column family 
    """
    namespace = namespace.strip()
    if len(namespace) > 0 :
        table = "{nm}:{tb}".format(nm=namespace,tb=hbase_table)
    else :
        table = hbase_table
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    conf = {"hbase.zookeeper.quorum": zk_hosts,"zookeeper.znode.parent": zk_parent,
        "hbase.mapred.outputtable": table,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    columns = df.columns
    rdd = df.rdd.flatMap(lambda r : flatRow(r,rowkey_col,column_family,columns))
    rdd.saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)

def readHbaseRDD(sc,zk_hosts,zk_parent,hbase_table):
    conf = {"hbase.zookeeper.quorum": zk_hosts, "zookeeper.znode.parent": zk_parent,
                "hbase.mapreduce.inputtable": hbase_table}
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
    hbase_rdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=keyConv,
        valueConverter=valueConv,
        conf=conf)
    return hbase_rdd

def jsonListToDict(r,rowkey_name):
    rowkey , jsonList = r
    dictResult = {rowkey_name:rowkey}
    for j in jsonList:
        dictResult[j['qualifier']] = j['value']
    return dictResult

def readHbaseTableToDataFrame(spark,zk_hosts,zk_parent,hbase_table,rowkey_name):
    sc = spark.sparkContext
    rdd = readHbaseRDD(sc,zk_hosts,zk_parent,hbase_table)
    #rdd2 = rdd.flatMap(lambda x : [ (x[0], json.loads(e) )  for e in x[1].split('\n') ] )
    # eval how much cols should be used 
    columns = rdd.flatMap(lambda x : [ (x[0], json.loads(e) )  for e in x[1].split('\n') ] ).map(lambda col : col[1]['qualifier']).distinct().collect()
    schema = StructType()
    schema.add(StructField(rowkey_name,StringType(),True))
    for c in columns :
        schema.add(StructField(c,StringType(),True))
    rdd2 = rdd.map(lambda x :  (x[0], [ json.loads(e)   for e in x[1].split('\n') ] )).map(lambda line: jsonListToDict(line,rowkey_name))
    df = spark.createDataFrame(rdd2,schema) 
    return df 

