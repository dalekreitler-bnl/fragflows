#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 17 09:00:15 2023

@author: dkreitler
"""

import os
import pandas
import subprocess

df = pandas.DataFrame()
find_cmd = ["find", "/data",
            "-maxdepth", "4", "-name", "*summary.csv"]
find_output = subprocess.check_output(find_cmd, universal_newlines=True)
csv_files = find_output.splitlines()

df = pandas.DataFrame()

for f in csv_files:
    df_to_add = pandas.read_csv(f)
    df_to_add['pipeline'] = ""
    for index, row in df_to_add.iterrows():
        df_to_add.at[index, 'pipeline'] = f.split('/')[-1].split('.')[0]
    df = pandas.concat([df, df_to_add], ignore_index=True)

# fix autoPROC -> autoProc
df['pipeline'][df['pipeline'] == 'autoPROC'] = 'autoProc'

# add xtal_id
df['xtal_id'] = ""
for index, row in df.iterrows():
    df.at[index, 'xtal_id'] = row['Sample_Path'].split('/')[0]

df_filtered = df[df['xtal_id'].str.contains("sample_name")]
final_df = df_filtered.groupby('xtal_id').apply(
    lambda group: group.loc[group['Hi'].idxmin()])

# get list of all reflection files
find_cmd = ['find', '/data',
            '-name', 'truncate-unique.mtz', '-o', '-name', 'fast_dp.mtz']
reflection_files = subprocess.check_output(
    find_cmd, universal_newlines=True).splitlines()

print(reflection_files)

# find file paths
final_df['filepath'] = ""
for index, row in final_df.iterrows():
    for f in reflection_files:
        if (row["Sample_Path"] in f) and (row["pipeline"] in f):
            final_df.at[index, "filepath"] = f

print(final_df)
