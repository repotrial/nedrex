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

trustrank_coll = DB["trustrank_"]
trustrank_dir = RESULTS_DIR / "trustrank_"
if not trustrank_dir.exists():
    trustrank_dir.mkdir(parents=False, exist_ok=True)


class TrustRankRequest(BaseModel):
    seeds: List[str] = Field(
        None,
        title="Seeds to use for trustrank",
        description="Protein seeds for trustrank; seeds should be UniProt accessions, (optionally prefixed with 'uniprot.')",
    )
    damping_factor: float = Field(
        None,
        title="The damping factor to use for trustrank",
        description="A float in the range 0 - 1. Default: `0.85`",
    )
    only_direct_drugs: bool = Field(None, title="", description="")
    only_approved_drugs: bool = Field(None, title="", description="")
    N: int = Field(
        None,
        title="Determines the number of candidates to return + store",
        description="After ordering (descending) by score, candidate drugs with a score >= the Nth drug's score are stored. Default: None",
    )

    class Config:
        extra = "forbid"


@router.post("/trustrank/submit")
async def trustrank_submit(
    background_tasks: BackgroundTasks, tr: TrustRankRequest = TrustRankRequest()
):
    if not tr.seeds:
        raise HTTPException(status_code=404, detail=f"No seed genes submitted")

    if tr.damping_factor is None:
        tr.damping_factor = 0.85
    if tr.only_direct_drugs is None:
        tr.only_direct_drugs = True
    if tr.only_approved_drugs is None:
        tr.only_approved_drugs = True

    query = {
        "seed_proteins": sorted([i.replace("uniprot.", "") for i in tr.seeds]),
        "damping_factor": tr.damping_factor,
        "only_direct_drugs": tr.only_direct_drugs,
        "only_approved_drugs": tr.only_approved_drugs,
        "N": tr.N,
    }

    result = trustrank_coll.find_one(query)

    if result:
        return result["uid"]

    query["uid"] = f"{uuid.uuid4()}"
    query["status"] = "submitted"

    with DB_LOCK:
        trustrank_coll.insert_one(query)

    background_tasks.add_task(run_trustrank_wrapper, query["uid"])
    return query["uid"]


@router.get("/trustrank/status")
def trustrank_status(uid: str):
    """
    Returns the details of the trustrank job with the given `uid`, including the original query parameters and the status of the build (`submitted`, `building`, `failed`, or `completed`).
    If the build fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = trustrank_coll.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/trustrank/download")
def trustrank_download(uid: str):
    query = {"uid": uid}
    result = trustrank_coll.find_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"No trustrank job with UID {uid}")
    if not result["status"] == "completed":
        raise HTTPException(
            status_code=404,
            detail=f"Trustrank job with UID {uid} does not have completed status",
        )
    return Response(
        (trustrank_dir / (uid + ".txt")).open("rb").read(),
        media_type="text/plain",
    )


def run_trustrank_wrapper(uid):
    try:
        run_trustrank(uid)
    except Exception as E:
        print(traceback.format_exc())
        with DB_LOCK:
            trustrank_coll.update_one(
                {"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}}
            )


def run_trustrank(uid):
    with DB_LOCK:
        details = trustrank_coll.find_one({"uid": uid})
        if not details:
            raise Exception()
        trustrank_coll.update_one({"uid": uid}, {"$set": {"status": "running"}})

    logger.info("Writing seeds to file")
    tmp = tempfile.NamedTemporaryFile(mode="wt")
    for seed in details["seed_proteins"]:
        tmp.write(f"uniprot.{seed}\n")
    tmp.flush()

    outfile = trustrank_dir / f"{uid}.txt"

    command = [
        settings.trustrank_run,
        "-n",
        f"{STATIC_DIR / 'PPDr-for-ranking.gt'}",
        "-s",
        f"{tmp.name}",
        "-d",
        f"{details['damping_factor']}",
        "-o",
        f"{outfile}",
    ]

    if details["only_direct_drugs"]:
        command.append("--only_direct_drugs")
    if details["only_approved_drugs"]:
        command.append("--only_approved_drugs")

    logger.info("Running trustrank in Docker container")
    res = subprocess.call(command)
    if res != 0:
        with DB_LOCK:
            trustrank_coll.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"Process exited with exit code {res} -- please contact API developer.",
                    }
                },
            )
        return

    logger.info("Finished running trustrank")
    if not details["N"]:
        with DB_LOCK:
            trustrank_coll.update_one({"uid": uid}, {"$set": {"status": "completed"}})
            return

    results = {}

    # Get results based on N.
    logger.info("Getting drugs from results")
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

    # Get the edges between seeds and drugs
    drug_ids = {i["drug_name"] for i in results["drugs"]}
    seeds = {f"uniprot.{i}" for i in details["seed_proteins"]}

    logger.info("Getting edges from network")
    # Parse network.
    G = nx.read_graphml(f"{STATIC_DIR / 'PPDr-for-ranking.graphml'}")
    for edge in product(drug_ids, seeds):
        if G.has_edge(*edge):
            results["edges"].append(list(edge))
    logger.info("Finished getting edges")

    trustrank_coll.update_one(
        {"uid": uid}, {"$set": {"status": "completed", "results": results}}
    )
