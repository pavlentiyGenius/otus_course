import sh
import boto3
import findspark
import pickle

import pydoop.hdfs as hdfs

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *

findspark.init()
print('Init Spark Session...')
spark = (SparkSession.builder
         .config(conf=SparkConf())
         .appName("local")
         .getOrCreate())

spark.conf.set('spark.sql.repl.eagerEval.enabled', True) 

schema = StructType([StructField("transaction_id", LongType(), True),
                     StructField("tx_datetime", TimestampType(), True),
                     StructField("customer_id", LongType(), True),
                     StructField("terminal_id", LongType(), True),
                     StructField("tx_amount", DoubleType(), True),
                     StructField("tx_time_seconds", LongType(), True),
                     StructField("tx_time_days", LongType(), True),
                     StructField("tx_fraud", LongType(), True),
                     StructField("tx_fraud_scenario", LongType(), True)])
print('Spark Session created.')

print('Reading Secrets..')
with open('s3_secrets.pickle', 'rb') as f:
    secrets = pickle.load(f)
print('Secrets Loaded.')

hdfsdir = './'
filelist = [ line.rsplit(None,1)[-1] for line in sh.hdfs('dfs','-ls',hdfsdir).split('\n') if len(line.rsplit(None,1))][1:]
filelist = [f for f in filelist if f.endswith('.txt')]
print('Count HDFS files for processing: ', len(filelist))

def check_file_exist(filename):
    '''
    Check for file existance.
    '''
    
    s3 = boto3.resource(aws_access_key_id=secrets['KEY_ID'], 
                    aws_secret_access_key=secrets['SECRET'], 
                    service_name='s3',
                    endpoint_url='https://storage.yandexcloud.net')

    bucket = s3.Bucket('mlops-bucket-parquet-20231030')
    files_in_bucket = list(bucket.objects.all())

    filelist_s3 = list(set([file.key.split('/')[0].split('.')[0] for file in files_in_bucket]))
    
    if filename in filelist_s3:
        return True
    else:
        return False

begin_size_all = 0
end_size_all = 0

print()
print('Start Preprocessing:')

for filename in filelist:
    print('Filename: ', filename)
    filename = filename.split('.')[0]
    
    if check_file_exist(filename):
        print(filename, ' already exist in mlops-bucket-parquet-20231030')
        print()
        continue
        
    df = spark.read.schema(schema)\
                .option("comment", "#")\
                .option("header", False)\
                .csv(f'{hdfs_host}{path}{filename}.txt')
    begin_size = df.count()
    print('Начальных размер DF: ', begin_size)
    df = df.na.drop("any") # очистка пустых значений
    df = df.dropDuplicates(['transaction_id']) # очистка дублей
    end_size = df.count()
    print('Итоговый размер DF: ', end_size)
    print('Дельта изменения количества записей: ', begin_size - end_size)
    
    df.write.parquet(f"{target_s3}{filename}.parquet", mode="overwrite")
#     df.write.parquet(f'{hdfs_host}{path}data_parquet/{filename}.parquet', mode="overwrite")
    print('Файл сохранен.')
    begin_size_all += begin_size
    end_size_all += end_size
    print()
    
print('Finish!')
print('Всего очищенных записей: ', begin_size_all - end_size_all)
