#!/usr/bin/env python

from collections.abc import MutableMapping
from pathlib import Path
from pprint import PrettyPrinter
import shutil
import subprocess
import sys
import uuid

from loguru import logger
import numpy as np
import pandas as pd
from pymongo import MongoClient

logger.remove()
logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:YYYY-MM-DD at HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
)


client = MongoClient("mongodb://localhost:27020")
db = client["test"]

EXCLUDE = ["bicon_", "closeness_", "diamond_", "graphs_", "must_", "trustrank_", "disorder_comorbid_with_disorder"]
NODES = tuple(i for i in db.list_collection_names() if len(i.split("_")) == 1 and i not in EXCLUDE)
EDGES = tuple(i for i in db.list_collection_names() if not len(i.split("_")) == 1 and i not in EXCLUDE)

DELIMITER = "|"

p = PrettyPrinter()

type_map = {
    bool : "boolean",
    int : "int",
    float : "double",
    str : "string"
}


def flatten(d, parent_key="", sep="."):
    """
    Function to flatten a nested dictionary.
    https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def calc_series_type(series_main):
    series = series_main.copy()
    series = series.dropna()

    s = set()
    for item in series:
        # Does item have content?
        if not item:
            continue

        # Is item a container?
        if isinstance(item, list):
            q = set(type(i) for i in item)
            assert len(q) == 1
            s.add( f"{type_map[q.pop()]}[]" )

        else:
            s.add( type_map[type(item)] )

    assert len(s) == 1
    return s.pop()

@logger.catch()
def main():
    workdir = Path("data/import")
    # Remove if already exists
    if workdir.exists():
        command = ["sudo", "rm", "-r", f"{workdir}"]
        subprocess.call(command)

    # Make the directory
    command = ["sudo", "mkdir", "-p", f"{workdir}"]
    subprocess.call(command)
    command = ["sudo", "chown", "-R", "james:james", f"{workdir}"]
    subprocess.call(command)

    # Remove existing graph DB if already exists
    neo_graphdb = Path("data/databases/graph.db/")
    if neo_graphdb.exists():
        # Remove
        command = ["sudo", "rm", "-r", f"{neo_graphdb}"]
        subprocess.call(command)

    for node in NODES:
        logger.info(f"Adding nodes of type {node}")
        cursor = db[node].find()
        # Construct DataFrame
        df = pd.DataFrame(flatten(i) for i in cursor)
        # Replace NaN with empty strings.
        df = df.replace(np.nan, '', regex=True)
        # Drop the ID column
        del df["_id"]
        if "_cls" in df.columns:
            del df["_cls"]

        # Change column names
        assert "primaryDomainId" in df.columns
        assert "type" in df.columns

        for col in df.columns:
            if col == "primaryDomainId":
                df = df.rename(columns={ col : f"{col}:ID"})
            elif col == "type":
                df["type:string"] = df["type"]
                df = df.rename(columns={ col : ":LABEL"})
            else:
                ty = calc_series_type(df[col])
                if ty.endswith("[]"): # If an array field, we'll exchange the column for the a string with delimiter.
                    df[col] = df[col].apply(DELIMITER.join)

                df = df.rename(columns={col: f"{col}:{ty}"})

        df.to_csv(f"{workdir}/{node}.csv", index=False)

    for edge in EDGES:
        logger.info(f"Adding edges of type {edge}")
        cursor = db[edge].find()
        # Construt DataFrame
        df = pd.DataFrame(flatten(i) for i in cursor)
        df = df.replace(np.nan, '', regex=True)
        # Drop the ID column
        del df["_id"]

        # Quick assertions to check the below blocks will work
        assert ( ("sourceDomainId" in df.columns) ^ ("memberOne" in df.columns) )
        assert ( ("targetDomainId" in df.columns) ^ ("memberTwo" in df.columns) )
        assert 'type' in df.columns

        for col in df.columns:
            if col in {"sourceDomainId", "memberOne"}:
                df = df.rename(columns={ col : f"{col}:START_ID"})
            elif col in {"targetDomainId", "memberTwo"}:
                df = df.rename(columns={ col : f"{col}:END_ID"})
            elif col == "type":
                # Copy the column.
                df["type:string"] = df["type"]
                df = df.rename(columns={ col : ":TYPE"})
            else:
                ty = calc_series_type(df[col])
                if ty.endswith("[]"): # If an array field, we'll exchange the column for the a string with delimiter.
                    df[col] = df[col].apply(DELIMITER.join)

                df = df.rename(columns={col: f"{col}:{ty}"})

        cols = list(df.columns)
        cols.remove(":TYPE")
        cols.append(":TYPE")
        df.to_csv(f"{workdir}/{edge}.csv", columns=cols, index=False)

    command = ["docker", "exec", "neo4j", "neo4j-admin", "import", f"--array-delimiter={DELIMITER}", "--multiline-fields=true"]
    for node in NODES:
        command += ["--nodes", f"data/import/{node}.csv"]
    for edge in EDGES:
        command += ["--relationships", f"data/import/{edge}.csv"]

    logger.info("Importing CSV files into Neo4j")
    subprocess.call(command)
    logger.info("Finished imports")

if __name__ == "__main__":
    main()
