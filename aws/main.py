# main.py
from pyspark.sql import SparkSession
from transform import read_data, explode_data_array, define_schema, create_new_dataframe, cast_numeric_columns, save_data_to_s3
from modules.reader import read_json
from modules.transformer import add_new_column, filter_rows
from modules.writer import write_to_parquet

def main():
    spark = SparkSession.builder \
        .appName("EV Data Processing Application") \
        .getOrCreate()

    # Define paths
    input_path = "s3://snowflake-ev-data/raw_data/data.json"
    output_path = "s3://snowflake-ev-data/transform_data"

    # Read data
    raw_data_df = read_data(spark, input_path)
    # explode the array
    df = explode_data_array(raw_data_df)
    df = df.select("data")

    # Define schema elements based on your data (modify as needed)
    data_item_schema = define_schema()

    # Create a new DataFrame with the applied schema
    new_df = create_new_dataframe(df, data_item_schema)

    # Cast numeric columns
    new_df = cast_numeric_columns(new_df)

    # Write data
    save_data_to_s3(new_df, output_path)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
