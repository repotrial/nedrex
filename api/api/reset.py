#!/usr/bin/env python

import subprocess
import time

CONDA_ENV = "/home/james/miniconda3/envs/repodb_v1/bin/python"


def get_repotrial_db_api():
    command = "ps aux | grep 'python run.py'"

    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    stdout, *_ = p.communicate()
    processes = [
        i
        for i in stdout.split(b"\n")
        if i and not b"grep" in i and not b"screen" in i.lower()
    ]

    # Check there's only one hit
    if len(processes) == 1:
        return processes[0].split()[1].decode()
    elif not processes:
        return None
    else:
        raise Exception("Unexpected number of processes!")


pid = get_repotrial_db_api()
command = ["kill", "-SIGINT", pid]
subprocess.call(command)

while get_repotrial_db_api():
    time.sleep(1)

command = ["screen", "-X", '-S', "api", "quit"]
subprocess.call(command)

time.sleep(1)
command = ["screen", "-d", "-m", "-S", "api", "bash", "-c", f'"{CONDA_ENV} run.py"']
command = " ".join(command)
subprocess.call(command, shell=True)
