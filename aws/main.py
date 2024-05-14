# main.py
from pyspark.sql import SparkSession
from modules.reader import read_json
from modules.transformer import add_new_column, filter_rows
from modules.writer import write_to_parquet

def main():
    spark = SparkSession.builder \
        .appName("Data Processing Application") \
        .getOrCreate()

    # Define paths
    input_path = "path_to_input_data.json"
    output_path = "path_to_output_data"

    # Read data
    df = read_json(spark, input_path)

    # Transform data
    df = add_new_column(df, "new_column", "literal('default_value')")
    df = filter_rows(df, "existing_column > 5")

    # Write data
    write_to_parquet(df, output_path)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
