def save_to_parquet(df, path, mode="overwrite", partition_by=None):
    """
    Saves a Spark DataFrame to S3 in Parquet format.

    Args:
      df: Spark DataFrame to be saved
      path: Path to the S3 location (including bucket and file name)
      mode: Write mode ("overwrite" or "append") - defaults to "overwrite"
      partition_by: List of column names to partition the data by (optional)
    """
    write_options = {"mode": mode}
    if partition_by:
        write_options["partitionBy"] = partition_by
    df.write.format("parquet").options(**write_options).save(path)
