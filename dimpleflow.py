# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import os
import tempfile
import subprocess
import shutil
import csv
import asyncio
from prefect import task, flow
from prefect.task_runners import ConcurrentTaskRunner
from pathlib import Path
import pandas

root_dir = Path("/data")
models_dir = root_dir / Path("models")
reference_model = str(root_dir / Path("reference.pdb"))
jobs_csv = root_dir / Path("filtered.csv")
jobs_df = pandas.read_csv(jobs_csv)
jobs_list = []

for index, row in jobs_df.iterrows():
    jobs_list.append(
        {
            "hklout": f"{row['xtal_id']}.dimple.mtz",
            "xyzout": f"{row['xtal_id']}.dimple.pdb",
            "xyzin": reference_model,
            "hklin": row["filepath"],
            "sample_dir": str(models_dir / Path(f"{row['xtal_id']}")),
            "xtal_id": row["xtal_id"],
        }
    )


@task(name="run_dimple", tags=["dimple_job"])
def run_dimple(dimple_params: dict):
    cmd = "dimple --hklout {hklout} --xyzout {xyzout} {xyzin} {hklin} {sample_dir}".format(
        **dimple_params
    )
    dimple_process = subprocess.Popen(
        cmd.split(),
        cwd=models_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    dimple_process.communicate()


@flow(name="dimple_flow", task_runner=ConcurrentTaskRunner)
def dimple_flow(jobs, **kwargs):
    run_dimple.map(jobs)


if __name__ == "__main__":
    job_chunks = [jobs_list[i : i + 30] for i in range(0, len(jobs_list), 30)]
    for chunk in job_chunks:
        dimple_flow(chunk)
