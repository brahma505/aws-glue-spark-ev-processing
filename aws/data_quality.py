# quality_checks.py
from pyspark.sql.functions import col


def check_for_nulls(df, column_list):
    """Check for null values in specified columns."""
    for column in column_list:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"Warning: Found {null_count} null values in column '{column}'.")
    return df


def check_for_duplicates(df, subset):
    """Check for duplicate rows based on a subset of columns."""
    initial_count = df.count()
    dedup_count = df.dropDuplicates(subset).count()
    if initial_count != dedup_count:
        print(
            f"Warning: Found {initial_count - dedup_count} duplicate rows based on columns {subset}."
        )
    return df


def check_value_ranges(df, column, min_val, max_val):
    """Check that values in a specific column fall within a specified range."""
    out_of_range_count = df.filter(
        (col(column) < min_val) | (col(column) > max_val)
    ).count()
    if out_of_range_count > 0:
        print(
            f"Warning: {out_of_range_count} out-of-range values found in column '{column}'."
        )
    return df
