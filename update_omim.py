import datetime
import json
import os
import time
from pathlib import Path
from shutil import copyfileobj
from tempfile import TemporaryDirectory as TD
from urllib.request import urlopen

import schedule
from dotenv import load_dotenv
from loguru import logger
from pymongo import MongoClient

import repodb

load_dotenv()
repodb.common.connect(port=int(os.environ["MONGO_PORT"]))


gawd_coll = "gene_associated_with_disorder"
metadata = "/home/james/repodb_v1/experimental/fastapi/refactored_api/static/metadata.json"

def update():
    cwd = str(Path().resolve())

    with TD() as tempdir:
        logger.info("Creating temporary directory")
        os.chdir(tempdir)
        logger.info("Obtaining new version of OMIM")
        get_new_omim()
        logger.info("Removing old OMIM data from Repotrial DB")
        remove_old_omim()
        logger.info("Updating new OMIM data from Repotrial DB")
        update_omim()
        logger.info("Updating metadata")
        update_metadata()
        logger.info("Done.")

    os.chdir(cwd)

def update_omim():
    op = repodb.parsers.omim.GeneMap2Parser("omim.txt")
    op.parse()


def update_metadata():
    with open(metadata, "r") as f:
        data = json.load(f)

    date = datetime.datetime.now().strftime("%Y-%m-%d")
    data["source_databases"]["omim"]["date"] = date
    v = str(data["version"])
    split_v = v.split(".")
    split_v[1] = str(int(split_v[1]) + 1)
    data["version"] = ".".join(split_v)

    with open(metadata, "w") as f:
        json.dump(data, f)



def remove_old_omim():
    client = MongoClient(f"localhost:{os.getenv('MONGO_PORT')}")
    db = client[os.getenv('MONGO_DB')]
    coll = db[gawd_coll]

    for item in coll.find():
        if set(item["assertedBy"]) == {"disgenet", "omim"}:
            coll.update_one(item, {"$pull": {"assertedBy": "omim"}})

        elif set(item["assertedBy"]) == {"omim"}:
            coll.delete_one(item)


def get_new_omim():
    with urlopen(os.getenv("GENEMAP_URL")) as response, open("omim.txt", "wb") as f:
        copyfileobj(response, f)


schedule.every().friday.at("02:00").do(update)

while True:
    schedule.run_pending()
    logger.debug("Nothing to do, sleeping for 1h")
    time.sleep(3600)
