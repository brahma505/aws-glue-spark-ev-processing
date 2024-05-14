from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType, StringType, ArrayType, IntegerType, LongType, BooleanType
from pyspark.sql import functions as F


# Define the Delta configuration as a dictionary
delta_conf = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.jars": "/usr/share/aws/delta/lib/delta-core.jar,/usr/share/aws/delta/lib/delta-storage.jar,/usr/share/aws/delta/lib/delta-storage-s3-dynamodb.jar",
    "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
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

s3_path = "s3://snowflake-ev-data/raw_data/data.json"
raw_data_df = read_data(spark, s3_path)

def explode_data_array(df):
  """
  Explodes the nested "data" array in the DataFrame.

  Args:
    df: Spark DataFrame containing the raw data

  Returns:
    Spark DataFrame with the "data" array exploded into separate rows
  """
  return df.withColumn("data", F.explode(col("data").cast(ArrayType(ArrayType(StringType())))))

df = explode_data_array(raw_data_df)
df = df.select("data")

def define_schema(hidden_flag_affects_usage=False):
  """
  Defines the schema for the data based on column names.

  Args:
    hidden_flag_affects_usage: Boolean flag indicating if "hidden" flag affects column usage (default: False)

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
    StructField(":@computed_region_8ddd_yn5v", StringType(), True),
  ])
  # Adjust nullability based on hidden_flag_affects_usage flag if needed
  return data_item_schema

# Define schema elements based on your data (modify as needed)
data_item_schema = define_schema()


def create_new_dataframe(df, schema):
  """
  Creates a new DataFrame with the specified schema applied to the exploded data.

  Args:
    df: Spark DataFrame containing the exploded data
    schema: StructType representing the desired schema

  Returns:
    Spark DataFrame with the schema applied
  """
  new_data = df.rdd.map(lambda row: Row(**dict(zip([f.name for f in schema.fields], 
                                                  [row.data[i] for i in range(len(schema.fields))]))))
  return spark.createDataFrame(new_data, schema)
# Create a new DataFrame with the applied schema
new_df = create_new_dataframe(df, data_item_schema)
def cast_numeric_columns(df):
  """
  Casts specific columns to numeric data types (IntegerType) based on column names.

  Args:
    df: Spark DataFrame containing the data with string type for numeric columns

  Returns:
    Spark DataFrame with numeric columns cast to IntegerType
  """
  return new_df.withColumn("electric_range", new_df["electric_range"].cast(IntegerType())).withColumn("base_msrp", new_df["base_msrp"].cast(IntegerType())).withColumn(":@computed_region_x4ys_rtnd", new_df[":@computed_region_x4ys_rtnd"].cast(IntegerType())).withColumn(":@computed_region_fny7_vc3j", new_df[":@computed_region_fny7_vc3j"].cast(IntegerType())).withColumn(":@computed_region_8ddd_yn5v", new_df[":@computed_region_8ddd_yn5v"].cast(IntegerType()))

new_df = cast_numeric_columns(new_df)

#save the dataframe to s3bucket in parquet format
new_df.write.mode("overwrite").parquet("s3://snowflake-ev-data/tranform_data/data.parquet")


dim_model_df = new_df.select("sid", "make", "model", "model_year", "ev_type", "cafv_type","electric_range", "base_msrp")
dim_model_df.withColumnRenamed("sid", "model_id").write.mode("overwrite").partitionBy("model_year").parquet("s3://snowflake-ev-data/processed_data/dim_model.parquet")

dim_county_df = new_df.select("sid", "county", "state")
dim_county_df.withColumnRenamed("sid", "county_id").write.mode("overwrite").parquet("s3://snowflake-ev-data/processed_data/dim_county.parquet")

dim_city_df =new_df.select("sid", "city", "state", "county","zip_code")
dim_city_df.withColumnRenamed("sid", "city_id").write.mode("overwrite").parquet("s3://snowflake-ev-data/processed_data/dim_city.parquet")

dim_electric_utility_df = new_df.select("sid", "electric_utility")
dim_electric_utility_df.withColumnRenamed("sid", "electric_utility_id").write.mode("overwrite").parquet("s3://snowflake-ev-data/processed_data/dim_electric_utility.parquet")

dim_legislative_district_df = new_df.select("sid", "legislative_district", "state")
dim_legislative_district_df.withColumnRenamed("sid", "legislative_district_id").write.mode("overwrite").parquet("s3://snowflake-ev-data/processed_data/dim_legislative_district.parquet")

dim_census_tract_geo_df = new_df.select("sid", "_2020_census_tract", "geocoded_column", ":@computed_region_x4ys_rtnd",":@computed_region_fny7_vc3j",":@computed_region_8ddd_yn5v")
dim_census_tract_geo_df.withColumnRenamed("sid", "census_tract_geo_id").write.mode("overwrite").parquet("s3://snowflake-ev-data/processed_data/dim_census_tract_geo.parquet")

#fact ev_registration table
ev_registation = new_df.alias("fact").select("fact.sid", "fact.id", "fact.created_at", "fact.updated_at","fact.vin_1_10","fact.dol_vehicle_id")
ev_registation.write.mode("overwrite").partitionBy("created_at").parquet("s3://snowflake-ev-data/processed_data/ev_registration.parquet")



