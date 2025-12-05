from pyspark.sql.functions import col, trim, regexp_replace, when, length, lit, to_date
from pyspark.sql.types import DoubleType, IntegerType, StringType


# Trim whitespace from multiple string columns.
def trim_string_columns(df, columns):
    for c in columns:
        df = df.withColumn(c, trim(col(c)))
    return df

# phone   
def normalize_phone(df, column):
    """
    Standardizes phone numbers:
    - Removes all non-digit characters
    - Valid if it contains 10 digits
    - Otherwise marked Invalid
    """
    df = df.withColumn(
        column,
        regexp_replace(col(column), r"[^\d]", "")  # keep only digits
    ).withColumn(
        column,
        when(length(col(column)) == 10, col(column))
        .otherwise("Invalid")
    )
    return df

# email
def validate_email(df, column):
    """
    Basic email pattern validation.
    Invalid -> set as null
    """
    email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

    df = df.withColumn(
        column,
        when(col(column).rlike(email_regex), col(column)).otherwise(None)
    )

    return df

# Convert column to date format.
def convert_to_date(df, column, fmt="yyyy-MM-dd"):
    df = df.withColumn(column, to_date(col(column), fmt))
    return df

# postal code
def clean_postal_code(df, column):
    """
    Standardizes postal codes:
    - Remove spaces
    - Accept 5-digit (US ZIP) or 6-digit (India)
    - Otherwise mark Invalid
    """
    df = df.withColumn(
        column,
        regexp_replace(col(column), r"\s+", "")  # remove spaces
    ).withColumn(
        column,
        when(length(col(column)).isin([5, 6]), col(column))  
        .otherwise("Invalid")
    )
    return df


# Keep only digits from the column.
# Set to null if final length < min_length.
def normalize_digits(df, column, min_length=8):
    clean_col = f"{column}_digits"

    df = df.withColumn(clean_col, regexp_replace(col(column), r"[^\d]", ""))
    df = df.withColumn(
        column,
        when(length(col(clean_col)) >= min_length, col(clean_col)).otherwise(lit(None))
    ).drop(clean_col)

    return df

# Cast columns to Double and set negative values to null.
def validate_numeric(df, columns):
    for c in columns:
        df = df.withColumn(c, col(c).cast(DoubleType()))
        df = df.withColumn(c, when(col(c) >= 0, col(c)).otherwise(lit(None)))
    return df


def validate_non_negative(df, columns):
    """
    Cast to double + ensure value >= 0
    """
    for c in columns:
        df = df.withColumn(c, col(c).cast(DoubleType()))
        df = df.withColumn(c, when(col(c).isNotNull() & (col(c) >= 0), col(c)).otherwise(None))
    return df

def validate_positive(df, columns):
    """Amount must be > 0."""
    for c in columns:
        df = df.withColumn(c, col(c).cast(DoubleType()))
        df = df.withColumn(c, when(col(c) > 0, col(c)).otherwise(None))
    return df


def validate_positive_integer(df, column):
    """
    Quantity must be > 0 integer.
    """
    df = df.withColumn(column, col(column).cast(IntegerType()))
    df = df.withColumn(
        column,
        when(col(column).isNotNull() & (col(column) > 0), col(column)).otherwise(None)
    )
    return df
