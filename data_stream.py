import logging
import logging.config
from configparser import ConfigParser
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

radio_schema = StructType([
    StructField("disposition_code", StringType(), True),
    StructField("description", StringType(), True)
])


def run_spark_job(spark):
    spark.sparkContext.setLogLevel("WARN")

    # Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "com.udacity.projects.sfcrime") \
        .option("startingOffsets", "earliest") \
        .option("maxRatePerPartition", 10) \
        .option("maxOffsetsPerTrigger", 2000) \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    # apply json schema to this json string and give alias of DF

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")

    # select original_crime_type_name and disposition

    distinct_table = service_table \
        .select("original_crime_type_name", "disposition", "call_date_time") \
        .withWatermark("call_date_time", "10 minutes")

    # count the number of original crime type
    agg_df = distinct_table.groupBy("original_crime_type_name").count().sort("count", ascending=False)

    #  Submit a screen shot of a batch ingestion of the aggregation
    #  write output stream
    query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # attach a ProgressReporter
    query.awaitTermination()

    # get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(path=radio_code_json_filepath, schema=radio_schema)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    #  rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_df = distinct_table \
        .join(radio_code_df, "disposition", "left") \
        .select("call_date_time", "original_crime_type_name", "description")

    # TODO join on disposition column
    join_query = join_df \
        .writeStream \
        .format("console") \
        .queryName("join") \
        .trigger(processingTime="10 seconds") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    config = ConfigParser()

    logging.config.fileConfig("logging.ini")
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", "3000") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
