import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def process_data(input_dir):
    spark = SparkSession.builder \
        .appName("ProcessProductView") \
        .getOrCreate()
        
    print(f"Starting Spark Job. Reading from {input_dir}")
    
    try:
        df = spark.read.csv(f"{input_dir}/*.csv", header=True, inferSchema=True)
        
        if 'id' in df.columns:
            df = df.withColumn("id", col("id").cast("integer"))
        elif 'product_id' in df.columns:
             df = df.withColumn("product_id", col("product_id").cast("integer"))
             
        if 'id' in df.columns:
            df = df.filter(col("id") > 0)
        elif 'product_id' in df.columns:
            df = df.filter(col("product_id") > 0)
            
        if '_kafka_timestamp' in df.columns:
            df = df.withColumn("event_time", to_timestamp(col("_kafka_timestamp") / 1000))
            
        print("Schema after transformations:")
        df.printSchema()
        
        print("Sample data:")
        df.show(5, truncate=False)
        
        output_path = f"{input_dir}/processed_output"
        print(f"Writing processed data to {output_path}")
        
        df.write.mode("overwrite").parquet(output_path)
        print("Spark Job completed successfully.")
        
    except Exception as e:
        print(f"Error processing data: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process product view data")
    parser.add_argument("--input-dir", required=True, help="Directory containing input CSV files")
    
    args = parser.parse_args()
    process_data(args.input_dir)
