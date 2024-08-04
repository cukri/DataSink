from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J
import os

os.environ['HADOOP_HOME'] = 'E:\dodatki\hadoop-3.3.6'
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')
os.environ['SPARK_HOME'] = 'E:/dodatki/spark-3.4.1'

if __name__ == "__main__":


    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.1") \
        .appName("HelloRDD")\
        .getOrCreate()

    logger = Log4J(spark)

    flightTimeParquetDF = spark.read\
        .format("parquet") \
        .load("E:/nauka/spark/DataSinkDemo/dataSource/flight-time.parquet")



    logger.info("Num Partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupby(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDF.repartition(5)
    logger.info("Num Partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    partitionedDF.groupby(spark_partition_id()).count().show()

    """partitionedDF.write \
        .format("avro") \
        .mode("overwrite") \
        .save("E:/nauka/spark/DataSinkDemo/dataSink/avro/")"""

    flightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite")\
        .option("path","dataSink/json/")\
        .partitionBy("OP_CARRIER", "ORIGIN")\
        .save()