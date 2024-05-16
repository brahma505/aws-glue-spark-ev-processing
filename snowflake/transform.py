from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, explode, array_construct
from snowflake.snowpark.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

# Snowflake configuration
sf_config = {
    "account": "<your_account>",
    "user": "<your_user>",
    "password": "<your_password>",
    "role": "<your_role>",
    "warehouse": "EV_WH",
    "database": "EV_DB",
    "schema": "EXTERNAL"
}

# Create Snowflake session
session = Session.builder.configs(sf_config).create()

def read_data(session, s3_path):
    """
    Reads data from the specified S3 path as JSON.

    Args:
      session: Snowflake session object
      s3_path: Path to the JSON data on S3

    Returns:
      Snowflake DataFrame containing the raw data
    """
    return session.read.option("FORMAT_NAME", "json_format").json(s3_path)

def explode_data_array(df):
    """
    Explodes the nested "data" array in the DataFrame.

    Args:
      df: Snowflake DataFrame containing the raw data

    Returns:
      Snowflake DataFrame with the "data" array exploded into separate rows
    """
    return df.with_column("data", explode(col("data")))

def define_schema():
    """
    Defines the schema for the data based on column names.

    Returns:
      StructType representing the schema
    """
    data_item_schema = StructType([
        StructField("sid", StringType(), True),
        StructField("id", StringType(), True),
        StructField("position", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("created_meta", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("updated_meta", StringType(), True),
        StructField("meta", StringType(), True),
        StructField("vin_1_10", StringType(), True),
        StructField("county", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("model_year", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("ev_type", StringType(), True),
        StructField("cafv_type", StringType(), True),
        StructField("electric_range", StringType(), True),
        StructField("base_msrp", StringType(), True),
        StructField("legislative_district", StringType(), True),
        StructField("dol_vehicle_id", StringType(), True),
        StructField("geocoded_column", StringType(), True),
        StructField("electric_utility", StringType(), True),
        StructField("_2020_census_tract", StringType(), True),
        StructField(":@computed_region_x4ys_rtnd", StringType(), True),
        StructField(":@computed_region_fny7_vc3j", StringType(), True),
        StructField(":@computed_region_8ddd_yn5v", StringType(), True)
    ])
    return data_item_schema

def create_new_dataframe(session, df, schema):
    """
    Creates a new DataFrame with the specified schema applied to the exploded data.

    Args:
      session: Snowflake session object
      df: Snowflake DataFrame containing the exploded data
      schema: StructType representing the desired schema

    Returns:
      Snowflake DataFrame with the schema applied
    """
    new_data = df.select(
        [col("data")[i].alias(schema[i].name) for i in range(len(schema.fields))]
    )
    return session.create_dataframe(new_data.collect(), schema=schema)

def cast_numeric_columns(df):
    """
    Casts specific columns to numeric data types (IntegerType) based on column names.

    Args:
      df: Snowflake DataFrame containing the data with string type for numeric columns

    Returns:
      Snowflake DataFrame with numeric columns cast to IntegerType
    """
    return (
        df.with_column("electric_range", df["electric_range"].cast(IntegerType()))
        .with_column("base_msrp", df["base_msrp"].cast(IntegerType()))
        .with_column(":@computed_region_x4ys_rtnd", df[":@computed_region_x4ys_rtnd"].cast(IntegerType()))
        .with_column(":@computed_region_fny7_vc3j", df[":@computed_region_fny7_vc3j"].cast(IntegerType()))
        .with_column(":@computed_region_8ddd_yn5v", df[":@computed_region_8ddd_yn5v"].cast(IntegerType()))
    )

def save_data_to_stage(session, new_df, stage_name):
    """
    Saves transformed data to a Snowflake stage.

    Args:
      session: Snowflake session object
      new_df: Snowflake DataFrame containing the transformed data
      stage_name: Name of the Snowflake stage

    Returns:
      None
    """
    new_df.write.mode("overwrite").save_as_table(stage_name)

def main():
    s3_path = "s3://snowflake-ev-data/raw_data/data.json"
    stage_name = "EV_DB.EXTERNAL.EV_PROCESSED"

    # Read and process data
    raw_df = read_data(session, s3_path)
    exploded_df = explode_data_array(raw_df)
    schema = define_schema()
    new_df = create_new_dataframe(session, exploded_df, schema)
    new_df = cast_numeric_columns(new_df)

    # Save data to Snowflake stage
    save_data_to_stage(session, new_df, stage_name)

if __name__ == "__main__":
    main()