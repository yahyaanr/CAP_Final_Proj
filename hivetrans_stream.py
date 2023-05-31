import pyarrow.hdfs as hdfs
from pyspark.sql import SparkSession, HiveContext

spark = (SparkSession
                .builder
                .appName('example-pyspark-read-and-write-from-hive')
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .config("spark.driver.extraClassPath", "/home/mocha/hive/lib/mysql-connector-java-8.0.15.jar")
                .config("spark.executor.extraClassPath", "/home/mocha/hive/lib/mysql-metadata-storage-0.9.2.jar")
                .enableHiveSupport()
                .getOrCreate()
                )

hive_context = HiveContext(spark)

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.defaultFS", "hdfs://localhost:9000")

hive_context.sql('CREATE DATABASE IF NOT EXISTS f_staging')
hive_context.sql("USE f_staging")

query ="""
CREATE EXTERNAL TABLE IF NOT EXISTS stream_stg2 (
    Timestamp STRING,
    StreamID STRING,
    StreamerUserID STRING,
    StreamerLoginName STRING,
    StreamerDisplayName STRING,
    StreamerStartTime STRING,
    StreamerTitle STRING,
    GameID STRING,
    GameName STRING,
    StreamType STRING,
    StreamLanguage STRING,
    Maturity STRING,
    ThumbnailURL STRING,
    ViewerCount STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/mocha/final/staging/stream_stg2'

"""
tables = hive_context.sql(query)

hdfs_dir = 'hdfs://localhost:9000/user/mocha/final/stream'
fs = hdfs.connect(host='localhost', port=9000)
file_count = len(fs.ls(hdfs_dir))

if file_count > 0:
    # Load data into the table
    query1 = """
    LOAD DATA INPATH 'hdfs://localhost:9000/user/mocha/final/stream/*.tsv' INTO TABLE stream_stg2
    """
    hive_context.sql(query1)  
else:
    print("No Parquet files found in the directory. Skipping data loading.")

hive_context.table("stream_stg2").show()

spark.stop()