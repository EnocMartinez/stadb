#!/usr/bin/env python3
"""
Utilities for the project

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 4/4/24
"""
from typing import Union

import rich
from rich.progress import Progress
import subprocess
import gc
import pandas as pd


def reverse_dictionary(data) -> dict:
    """
    Takes a dictionary and reverses key-value pairs
    :param data: any dict
    :return: reversed dictionary
    """
    return {value: key for key, value in data.items()}


def run_subprocess(cmd, fail_exit:bool=False) -> bool:
    """
    Runs a command as a subprocess. If the process retunrs 0 returns True. Otherwise prints stderr and stdout and returns False
    :param cmd: command (list or string)
    :return: True/False
    """
    assert (type(cmd) is list or type(cmd) is str)
    if type(cmd) == list:
        cmd_list = cmd
    else:
        cmd_list = cmd.split(" ")
    proc = subprocess.run(cmd_list, capture_output=True)
    if proc.returncode != 0:
        rich.print(f"\n[red]ERROR while running command '{cmd}'")
        if proc.stdout:
            rich.print(f"subprocess stdout:")
            rich.print(f">[bright_black]    {proc.stdout.decode()}")
        if proc.stderr:
            rich.print(f"subprocess stderr:")
            rich.print(f">[bright_black] {proc.stderr.decode()}")

        if fail_exit:
            exit(1)

        return False
    return True


def merge_dataframes_by_columns(dataframes: list, timestamp="timestamp"):
    """
    Merge a list of dataframes to a single dataframe by merging on timestamp
    :param dataframes:
    :param timestamp:
    :return:
    """
    if len(dataframes) < 1:
        raise ValueError(f"Got {len(dataframes)} dataframes in list!")
    df = dataframes[0]
    for i in range(1, len(dataframes)):
        temp_df = dataframes[i].copy()
        df = df.merge(temp_df, on=timestamp, how="outer")
        del temp_df
        dataframes[i] = None
        gc.collect()
    return df


def merge_dataframes(df_list, sort=False)-> pd.DataFrame:
    """
    Appends together several dataframes into a big dataframe
    :param df_list: list of dataframes to be appended together
    :param sort: If True, the resulting dataframe will be sorted based on its index
    :return:
    """
    df = df_list[0]
    for new_df in df_list[1:]:
        df = df.append(new_df)

    if sort:
        df = df.sort_index(ascending=True)
    return df


def dataframe_to_dict(df, key, value)->dict:
    """
    Takes two columns of a dataframe and converts it to a dictionary
    :param df: input dataframe
    :param key: column name that will be the key
    :param value: column name that will be the value
    :return: dict
    """
    keys = df[key]
    values = df[value]
    d = {}
    for i in range(len(keys)):
        d[keys[i]] = values[i]
    return d


def slice_dataframes(df, max_rows=-1, frequency=""):
    """
    Slices input dataframe into multiple dataframe, making sure than every dataframe has at most "max_rows"
    :param df: input dataframe
    :param max_rows: max rows en every dataframe
    :param frequency: M for month, W for week, etc.
    :return: list with dataframes
    """
    if max_rows < 0 and not frequency:
        raise ValueError("Specify max rows or a frequency")

    if max_rows > 0:
        if len(df.index.values) > max_rows:
            length = len(df.index.values)
            i = 0
            dataframes = []
            with Progress() as progress:
                task = progress.add_task("slicing dataframes...", total=length)
                while i + max_rows < length:
                    init = i
                    end = i + max_rows
                    newdf = df.iloc[init:end]
                    dataframes.append(newdf)
                    i += max_rows
                    progress.update(task, advance=max_rows)
                newdf = df.iloc[i:]
                dataframes.append(newdf)
                progress.update(task, advance=(length-i))
        else:
            dataframes = [df]
    else:  # split by frequency
        dataframes = [g for n, g in df.groupby(pd.Grouper(freq=frequency))]

    # ensure that we do not have empty dataframes
    dataframes = [df for df in dataframes if not df.empty]
    rich.print("[green]sliced!")
    return dataframes