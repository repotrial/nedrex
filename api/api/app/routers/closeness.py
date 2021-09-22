import csv
import subprocess
import tempfile
import traceback
import uuid
from itertools import product
from typing import List

import networkx as nx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from pydantic import BaseModel, Field

from app.common import DB, DB_LOCK, RESULTS_DIR, STATIC_DIR, logger, settings

router = APIRouter()

closeness_coll = DB["closeness_"]
closeness_dir = RESULTS_DIR / "closeness_"
if not closeness_dir.exists():
    closeness_dir.mkdir(parents=False, exist_ok=True)


class ClosenessRequest(BaseModel):
    seeds: List[str] = Field(
        None,
        title="Seeds to use for trustrank",
        description="Protein seeds for trustrank; seeds should be UniProt accessions, (optionally prefixed with 'uniprot.')",
    )
    only_direct_drugs: bool = Field(None)
    only_approved_drugs: bool = Field(None)
    N: int = Field(
        None,
        title="Determines the number of candidates to return + store",
        description="After ordering (descending) by score, candidate drugs with a score >= the Nth drug's score are stored. Default: None",
    )

    class Config:
        extra = "forbid"


@router.post("/closeness/submit")
async def closeness_submit(
    background_tasks: BackgroundTasks, tr: ClosenessRequest = ClosenessRequest()
):
    if not tr.seeds:
        raise HTTPException(status_code=404, detail=f"No seed genes submitted")
    if tr.only_direct_drugs is None:
        tr.only_direct_drugs = True
    if tr.only_approved_drugs is None:
        tr.only_approved_drugs = True

    query = {
        "seed_proteins": sorted([i.replace("uniprot.", "") for i in tr.seeds]),
        "only_direct_drugs": tr.only_direct_drugs,
        "only_approved_drugs": tr.only_approved_drugs,
        "N": tr.N,
    }

    result = closeness_coll.find_one(query)

    if result:
        return result["uid"]

    query["uid"] = f"{uuid.uuid4()}"
    query["status"] = "submitted"

    with DB_LOCK:
        closeness_coll.insert_one(query)

    background_tasks.add_task(run_closeness_wrapper, query["uid"])
    return query["uid"]


@router.get("/closeness/status")
def closeness_status(uid: str):
    """
    Returns the details of the closeness job with the given `uid`, including the original query parameters and the status of the build (`submitted`, `building`, `failed`, or `completed`).
    If the build fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = closeness_coll.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/closeness/download")
def closeness_download(uid: str):
    query = {"uid": uid}
    result = closeness_coll.find_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"No closness job with UID {uid}")
    if not result["status"] == "completed":
        raise HTTPException(
            status_code=404,
            detail=f"Closeness job with UID {uid} does not have completed status",
        )
    return Response(
        (closeness_dir / (uid + ".txt")).open("rb").read(),
        media_type="text/plain",
    )


def run_closeness_wrapper(uid):
    try:
        run_closeness(uid)
    except Exception as E:
        print(traceback.format_exc())
        with DB_LOCK:
            closeness_coll.update_one(
                {"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}}
            )


def run_closeness(uid):
    with DB_LOCK:
        details = closeness_coll.find_one({"uid": uid})
        if not details:
            raise Exception()
        closeness_coll.update_one(
            {"uid": uid}, {"$set": {"status": "running"}}
        )

    logger.info("Writing seeds to file.")
    tmp = tempfile.NamedTemporaryFile(mode="wt")
    for seed in details["seed_proteins"]:
        tmp.write(f"uniprot.{seed}\n")
    tmp.flush()

    outfile = closeness_dir / f"{uid}.txt"

    command = [
        settings.closeness_run,
        "-n",
        f"{STATIC_DIR / 'PPDr-for-ranking.gt'}",
        "-s",
        f"{tmp.name}",
        "-o",
        f"{outfile}",
    ]
    if details["only_direct_drugs"]:
        command.append("--only_direct_drugs")
    if details["only_approved_drugs"]:
        command.append("--only_approved_drugs")

    logger.info("Running closeness in Docker container")
    res = subprocess.call(command)
    tmp.close()
    logger.info("Finished running closeness")

    if res != 0:
        with DB_LOCK:
            closeness_coll.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"Process exited with exit code {res} -- please contact API developer.",
                    }
                },
            )
        return

    if not details["N"]:
        with DB_LOCK:
            closeness_coll.update_one(
                {"uid": uid}, {"$set": {"status": "completed"}}
            )
        return

    results = {}

    logger.info("Getting drugs from results")
    # Get results based on N.
    with outfile.open("r") as f:
        keep = []
        reader = csv.DictReader(f, delimiter="\t")
        for _ in range(details["N"]):
            item = next(reader)
            if float(item["score"]) == 0:
                break
            keep.append(item)

        lowest_score = keep[-1]["score"]
        if float(lowest_score) != 0:
            while True:
                item = next(reader)
                if item["score"] != lowest_score:
                    break
                keep.append(item)

    results["drugs"] = keep
    results["edges"] = []

    logger.info("Getting edges from network")
    # Get the edges between seeds and drugs
    drug_ids = {i["drug_name"] for i in results["drugs"]}
    seeds = {f"uniprot.{i}" for i in details["seed_proteins"]}

    # Parse network.
    G = nx.read_graphml(f"{STATIC_DIR / 'PPDr-for-ranking.graphml'}")
    for edge in product(drug_ids, seeds):
        if G.has_edge(*edge):
            results["edges"].append(list(edge))
    logger.info("Finished getting edges")

    with DB_LOCK:
        closeness_coll.update_one(
            {"uid": uid}, {"$set": {"status": "completed", "results": results}}
        )
