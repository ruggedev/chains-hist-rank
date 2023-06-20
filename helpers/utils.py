from collections import Counter
from typing import List
import functools as ft
import pandas as pd
import glob
import os


def unique_list(old_list) -> List:
    """
    :param old_list:
    :return: unique list
    """
    return [*Counter(old_list)]


def merge_df(_dfs, _key) -> pd.DataFrame:
    """
    :param _dfs: list of pandas Dataframe
    :param _key: key column for merging
    :return: merged Dataframe
    """
    return ft.reduce(lambda left, right: pd.merge(left, right, on=_key), _dfs)


def merge_csv_by_path(dir_path, dest, sort_by=None, order=None) -> None:
    """
    :param dir_path: path of the directory
    :param dest: output destination
    :param sort_by: columns to be sorted, e.g. ['name, 'date']
    :param order: order directions, e.g. [True, True]
    :return: merged csv
    """
    all_csv = glob.glob(os.path.join(dir_path, '*.csv'))
    merged_df = pd.concat(map(pd.read_csv, all_csv), ignore_index=True)
    if sort_by:
        merged_df = merged_df.sort_values(sort_by, ascending=order)

    merged_df.to_csv(dest, index=False)


def safe_exists(data, dtype=str, empty_value='') -> bool:
    """
    :param data: data to check
    :param dtype: expected data type
    :param empty_value: empty value
    :return: data exists or not
    notice: ensure the data match dtype and not null and not equal to its default value
    """
    if data and isinstance(data, dtype) and data != empty_value:
        return True
    else:
        return False


def safe_return(data, dtype=None, dafault_value=None):
    try:
        if dtype:
            return dtype(data)
        else:
            return data
    except:
        return dafault_value


def get_value_from_df(df, search_k, search_v, target_k):
    """
    :param df: targe dataframe
    :param search_k: search column
    :param search_v: search value
    :param target_k: target column
    :return: target value
    """
    try:
        if (df.loc[df[search_k] == search_v, target_k]).any():
            target_v = df.loc[df[search_k] == search_v, target_k].values[0]
            return target_v
    except:
        return None