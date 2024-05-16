from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    TimestampType,
    StringType,
    ArrayType,
    IntegerType,
    LongType,
    BooleanType,
)
from pyspark.sql import functions as F


# Define the Delta configuration as a dictionary
delta_conf = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.jars": "/usr/share/aws/delta/lib/delta-core.jar,/usr/share/aws/delta/lib/delta-storage.jar,/usr/share/aws/delta/lib/delta-storage-s3-dynamodb.jar",
    "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
}

# Create SparkSession with the Delta configuration
spark = SparkSession.builder.config(conf=delta_conf).getOrCreate()


def read_data(spark, s3_path):
    """
    Reads data from the specified S3 path as JSON and allows unquoted field names.

    Args:
      spark: SparkSession object
      s3_path: Path to the JSON data on S3

    Returns:
      Spark DataFrame containing the raw data
    """
    return spark.read.option("allowUnquotedFieldNames", "true").json(s3_path)


def explode_data_array(df):
    """
    Explodes the nested "data" array in the DataFrame.

    Args:
      df: Spark DataFrame containing the raw data

    Returns:
      Spark DataFrame with the "data" array exploded into separate rows
    """
    return df.withColumn(
        "data", F.explode(col("data").cast(ArrayType(ArrayType(StringType()))))
    )


def define_schema(hidden_flag_affects_usage=False):
    """
    Defines the schema for the data based on column names.

    Args:
      hidden_flag_affects_usage: Boolean flag indicating if "hidden" flag affects column usage (default: False)

    Returns:
      StructType representing the schema
    """
    data_item_schema = StructType(
        [
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
            StructField(":@computed_region_8ddd_yn5v", StringType(), True),
        ]
    )
    # Adjust nullability based on hidden_flag_affects_usage flag if needed
    return data_item_schema


def create_new_dataframe(df, schema):
    """
    Creates a new DataFrame with the specified schema applied to the exploded data.

    Args:
      df: Spark DataFrame containing the exploded data
      schema: StructType representing the desired schema

    Returns:
      Spark DataFrame with the schema applied
    """
    new_data = df.rdd.map(
        lambda row: Row(
            **dict(
                zip(
                    [f.name for f in schema.fields],
                    [row.data[i] for i in range(len(schema.fields))],
                )
            )
        )
    )
    return spark.createDataFrame(new_data, schema)


def cast_numeric_columns(df):
    """
    Casts specific columns to numeric data types (IntegerType) based on column names.

    Args:
      df: Spark DataFrame containing the data with string type for numeric columns

    Returns:
      Spark DataFrame with numeric columns cast to IntegerType
    """
    return (
        df.withColumn("electric_range", df["electric_range"].cast(IntegerType()))
        .withColumn("base_msrp", df["base_msrp"].cast(IntegerType()))
        .withColumn(
            ":@computed_region_x4ys_rtnd",
            df[":@computed_region_x4ys_rtnd"].cast(IntegerType()),
        )
        .withColumn(
            ":@computed_region_fny7_vc3j",
            df[":@computed_region_fny7_vc3j"].cast(IntegerType()),
        )
        .withColumn(
            ":@computed_region_8ddd_yn5v",
            df[":@computed_region_8ddd_yn5v"].cast(IntegerType()),
        )
    )


# save the dataframe to s3bucket in parquet format
def save_data_to_s3(new_df, output_path):
    """
    Saves transformed data to parquet files in S3 bucket

    Args:
        new_df (pyspark.sql.DataFrame): The transformed DataFrame to be saved
    """

    # Save full transformed data
    new_df.write.mode("overwrite").parquet(f"{output_path}/data.parquet")

    # Define dimension tables and their column selections
    dimension_tables = {
        "dim_model": (
            [
                "sid",
                "make",
                "model",
                "model_year",
                "ev_type",
                "cafv_type",
                "electric_range",
                "base_msrp",
            ],
            "model_id",
        ),
        "dim_county": (["sid", "county", "state"], "county_id"),
        "dim_city": (["sid", "city", "state", "county", "zip_code"], "city_id"),
        "dim_electric_utility": (["sid", "electric_utility"], "electric_utility_id"),
        "dim_legislative_district": (
            ["sid", "legislative_district", "state"],
            "legislative_district_id",
        ),
        "dim_census_tract_geo": (
            [
                "sid",
                "_2020_census_tract",
                "geocoded_column",
                ":@computed_region_x4ys_rtnd",
                ":@computed_region_fny7_vc3j",
                ":@computed_region_8ddd_yn5v",
            ],
            "census_tract_geo_id",
        ),
    }

    # Save dimension tables with partitioning and renaming
    for table_name, (selected_columns, renamed_id_column) in dimension_tables.items():
        df = new_df.select(*selected_columns)
        df = df.withColumnRenamed(
            selected_columns[0], renamed_id_column
        )  # Rename the first column (sid)
        df.write.mode("overwrite").partitionBy(
            "model_year" if table_name == "dim_model" else None
        ).parquet(f"s3://snowflake-ev-data/processed_data/{table_name}.parquet")
        return

    # Save fact table with partitioning
    ev_registration = new_df.alias("fact").select(
        "fact.sid",
        "fact.id",
        "fact.created_at",
        "fact.updated_at",
        "fact.vin_1_10",
        "fact.dol_vehicle_id",
    )
    ev_registration.write.mode("overwrite").partitionBy("created_at").parquet(
        "s3://snowflake-ev-data/processed_data/ev_registration.parquet"
    )
