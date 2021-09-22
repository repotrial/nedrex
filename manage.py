#!/usr/bin/env python

from pathlib import Path

import click
import loguru
import pymongo
import semantic_version

client = pymongo.MongoClient("localhost:27020")
db = client["test"]

results_dir = Path("experimental/fastapi/refactored_api/results")
logger = loguru.logger


@click.group()
def cli():
    pass


@click.group(help="Operations related to graphs build by Repotrial DB")
def graphs():
    pass


@click.group(help="Operations related to results stored in Repotrial DB")
def results():
    pass


@results.command(
    help="Removes the result with the specified UID from the target results"
)
@click.option(
    "-c",
    "--collection",
    type=click.Choice(
        [
            "diamond",
            "bicon",
            "closeness",
            "graphs",
            "must",
            "trustrank",
            "graphs",
        ]
    ),
)
@click.option("-u", "--uid", type=str)
def remove(collection, uid):
    coll = db[f"{collection}_"]
    res = coll.delete_one({"uid": uid})
    if not res.deleted_count == 1:
        logger.warning(
            f"uid {uid} does not appear to be in collection {collection}"
        )

    relevant_directory = results_dir / f"{collection}_"
    for f in relevant_directory.iterdir():
        if f.name.startswith(uid):
            logger.info(f"Removing {f}")
            f.unlink()


@graphs.command(help="Removes graphs built before a specified version")
def clean():
    value = click.prompt(
        "Please enter the version you wish to delete up to (not including)",
        type=str,
    )
    confirm_value = click.prompt(
        "Please confirm the version you wish to delete up to (not including)",
        type=str,
    )

    if value != confirm_value:
        print("Mismatch in values -- please check and retry.")
        return

    try:
        v = semantic_version.Version(value)
    except ValueError:
        print("Version given is not valid -- please check an retry.")
        return

    # Get versions that are lower than the specified version.
    coll = db["graphs_"]
    uids = {
        entry["uid"]
        for entry in coll.find()
        if semantic_version.Version(entry["version"]) < v
    }
    graph_dir = results_dir / "graphs_"

    warning_count = 0

    for uid in uids:
        q = graph_dir / f"{uid}.graphml"
        coll.delete_one({"uid": uid})
        if q.exists():
            q.unlink()
            logger.debug(f"{uid} successfully removed")
        else:
            logger.warning(f"{uid} did not have an associated graphml file.")
            warning_count += 1

    logger.info(f"Removed {len(uids)} entries ({warning_count} warnings).")


cli.add_command(graphs)
cli.add_command(results)


if __name__ == "__main__":
    cli()
