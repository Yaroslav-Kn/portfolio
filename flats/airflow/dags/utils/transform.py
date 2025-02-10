import pandas as pd


def del_small_target(data: pd.DataFrame, treshold: int) -> pd.DataFrame:
    new_data = data.copy(deep=True)
    return new_data.loc[new_data['target'] > treshold]

def remove_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    feature_cols = data.columns.drop('flat_id').tolist()
    data = data.drop_duplicates(subset=feature_cols)
    return data 

def del_outliers(data: pd.DataFrame, ignor_list: list[str], threshold: int = 1.5) -> pd.DataFrame:
    num_columns = data.select_dtypes(['float', 'int'])
    num_columns = num_columns.columns.drop(ignor_list).tolist()
    potential_outliers = pd.DataFrame()

    for col in num_columns:
        q1 = data[col].quantile(0.25)
        q3 = data[col].quantile(0.75)
        iqr =  q3 - q1
        margin = threshold * iqr
        lower = q1 - margin
        upper = q3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)

    outliers = potential_outliers.any(axis=1)

    return data[~outliers]

def get_relative_flat(data: pd.DataFrame) -> pd.DataFrame:
    new_data = data.copy(deep=True)
    relative_flat = new_data['floor'] / new_data['floors_total']
    new_data.insert(new_data.shape[1] - 1, 'relative_floor', relative_flat)
    return new_data
