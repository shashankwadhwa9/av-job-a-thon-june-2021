from datetime import datetime


def clean_visit_datetime_col(val):
    if val is not None and val.isdigit():
        return datetime.fromtimestamp(int(val)/1000000000).strftime('%Y-%m-%d %H:%M:%S.%f')

    return val
