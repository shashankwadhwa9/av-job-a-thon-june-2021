from datetime import datetime
from pyspark.sql import functions as F


def clean_visit_datetime_col(val):
    if val is not None and val.isdigit():
        return datetime.fromtimestamp(int(val)/1000000000).strftime('%Y-%m-%d %H:%M:%S.%f')

    return val


visit_datetime_normalize_udf = F.udf(lambda x: clean_visit_datetime_col(x))
