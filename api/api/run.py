#!/usr/bin/env python

import os
from pathlib import Path
from urllib import request

import sh
import uvicorn
from dotenv import load_dotenv
from uvicorn.config import LOGGING_CONFIG

load_dotenv()

static_dl = "https://nc.repotrial.bioswarm.net/s/is94W7kd3dA5enr/download"
static_md5 = "https://nc.repotrial.bioswarm.net/s/jZF2RgtMTTAnNHX/download"

app_string = "app.main:app"

def run():
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    uvicorn.run(app_string, port=int(os.getenv("API_PORT")), reload=True, host="0.0.0.0")


def create_results_directory_structure():
    # Check to see if the results directory structure exists.
    # If it doesn't, create it.
    Path("results/bicon_").mkdir(parents=True, exist_ok=True)
    Path("results/closeness_").mkdir(parents=True, exist_ok=True)
    Path("results/diamond_").mkdir(parents=True, exist_ok=True)
    Path("results/must_").mkdir(parents=True, exist_ok=True)
    Path("results/trustrank_").mkdir(parents=True, exist_ok=True)
    Path("results/graphs_").mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(parents=True, exist_ok=True)

def obtain_static_files():
    # Check to see if the static files exist, and create if not.
    if Path("static").exists():
        return
    request.urlretrieve(static_dl, "static.tar.gz")
    request.urlretrieve(static_md5, "check.md5")

    # Do an md5sum check to make sure static files downloaded OK.
    result = sh.md5sum("-c", "check.md5")
    # TODO: Decide what to do on failure.
    assert "static.tar.gz: OK" in result

    # Remove md5sum check file, decompress tar.gz, and delete archive.
    os.remove("check.md5")
    sh.tar("zxvf", "static.tar.gz")
    os.remove("static.tar.gz")


if __name__ == '__main__':
    create_results_directory_structure()
    obtain_static_files()

    from app.main import app
    run()
