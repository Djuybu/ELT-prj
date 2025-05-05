#!/usr/bin/env python3
# kafka_to_gcs_processor.py - Spark Streaming script to process Kafka product data to GCS

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, DoubleType, TimestampType
import argparse
import logging
import time
from typing import List, Optional

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Process Kafka product data to GCS')
    parser.add_argument('--kafka_brokers', default='localhost:9092', help='Kafka brokers (comma-separated)')
    parser.add_argument('--kafka_topic', default='new_product', help='Kafka topic name')
    parser.add_argument('--gcs_path', default='gs://bigdata-team3-uet-zz/bronze/products', help='GCS output path')
    parser.add_argument('--checkpoint_path', default='gs://bigdata-team3-uet-zz/checkpoints/products', help='Checkpoint location')
    parser.add_argument('--timeout_minutes', type=int, default=30, help='Timeout in minutes (0 for indefinite)')
    return parser.parse_args()

def main():
    """Main function to run the Spark streaming job"""
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting Kafka to GCS product data processing")
    logger.info(f"Kafka Brokers: {args.kafka_brokers}")
    logger.info(f"Kafka Topic: {args.kafka_topic}")
    logger.info(f"GCS Path: {args.gcs_path}")
    logger.info(f"Timeout: {args.timeout_minutes} minutes")
    
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Kafka-GCS-Products-Pipeline") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    try:
        # Define schema for the Kafka message value (ProductDto)
        product_schema = StructType([
            StructField("productId", IntegerType(), True),
            StructField("ProductName", StringType(), True),
            StructField("brandName", StringType(), True),  
            StructField("Description", StringType(), True),
            StructField("Price", FloatType(), True),
            StructField("Weight", FloatType(), True),
            StructField("Img", StringType(), True),
            StructField("categoryIds", ArrayType(IntegerType()), True)
        ])
        
        # Read from Kafka
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.kafka_brokers) \
            .option("subscribe", args.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON from Kafka
        product_df = kafka_df \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), product_schema).alias("product")) \
            .select("product.*")
        
        # Register UDF for extracting category levels
        @udf(returnType=StringType())
        def get_category_level(category_ids: Optional[List[int]], level: int) -> Optional[str]:
            if category_ids and len(category_ids) >= level:
                return str(category_ids[level - 1])
            return None
        
        # Transform data to match target schema
        transformed_df = product_df \
            .withColumn("product_id", col("productId").cast(StringType())) \
            .withColumn("level_1", get_category_level(col("categoryIds"), lit(1))) \
            .withColumn("level_2", get_category_level(col("categoryIds"), lit(2))) \
            .withColumn("level_3", get_category_level(col("categoryIds"), lit(3))) \
            .withColumn("level_4", get_category_level(col("categoryIds"), lit(4))) \
            .withColumn("brand_name", col("brandName")) \
            .withColumn("product_name", col("ProductName")) \
            .withColumn("product_price", col("Price").cast(DoubleType())) \
            .withColumn("first_issue_date", current_timestamp()) \
            .select(
                "product_id", "level_1", "level_2", "level_3", "level_4", 
                "brand_name", "product_name", "product_price", "first_issue_date"
            )
        
        # Write to Google Cloud Storage in Parquet format
        query = transformed_df.writeStream \
            .format("parquet") \
            .option("path", args.gcs_path) \
            .option("checkpointLocation", args.checkpoint_path) \
            .partitionBy("first_issue_date") \
            .trigger(processingTime="1 minute") \
            .outputMode("append") \
            .start()
        
        # Calculate timeout in milliseconds
        timeout_ms = args.timeout_minutes * 60 * 1000 if args.timeout_minutes > 0 else -1
        
        # If running with a timeout
        if timeout_ms > 0:
            logger.info(f"Running with timeout of {args.timeout_minutes} minutes")
            start_time = time.time()
            
            try:
                query.awaitTermination(timeout_ms)
                logger.info(f"Streaming job completed after {args.timeout_minutes} minutes")
            except Exception as e:
                logger.error(f"Error during stream processing: {str(e)}")
                raise
        else:
            # Run indefinitely
            logger.info("Running indefinitely until manual termination")
            query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error in Spark streaming job: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
