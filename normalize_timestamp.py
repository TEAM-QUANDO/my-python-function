import pandas as pd

def normalize_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    dt_columns = df.select_dtypes('datetimetz')
    df[dt_columns.columns] = dt_columns.apply(lambda x: x.dt.tz_convert('utc'))
    dt_columns = df.select_dtypes('datetime64')
    df[dt_columns.columns] = dt_columns.apply(lambda x: pd.to_datetime(x, utc=True))
    df[dt_columns.columns] = dt_columns.apply(lambda x: x.dt.strftime('%Y-%m-%dT%H:%M:%S+00:00'))
    return df