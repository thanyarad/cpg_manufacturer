from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re

@udf(returnType=BooleanType())
def is_valid_sku(sku):
    if sku is None:
        return False
    pattern = r'^[\w\s\-]+$'
    return re.match(pattern, sku) is not None
