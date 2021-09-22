import csv
import shutil
import subprocess
import tempfile
import time
import traceback
import uuid
from itertools import product
from typing import List

from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from pydantic import BaseModel, Field

from app.common import DB_LOCK, DB, RESULTS_DIR, get_network, logger, settings

router = APIRouter()

must_coll = DB["must_"]
must_dir = RESULTS_DIR / "must_"
if not must_dir.exists():
    must_dir.mkdir(parents=False, exist_ok=True)

default_query = """
MATCH (pa)-[ppi:ProteinInteractsWithProtein]-(pb)
WHERE "exp" in ppi.evidenceTypes
MATCH (pa)-[:ProteinEncodedBy]->(x)
MATCH (pb)-[:ProteinEncodedBy]->(y)
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

default_query_protein = """
MATCH (x)-[ppi:ProteinInteractsWithProtein]-(y)
WHERE "exp" in ppi.evidenceTypes
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

shared_disorder_query = """
MATCH (x:Gene)-[:GeneAssociatedWithDisorder]->(d:Disorder)
MATCH (y:Gene)-[:GeneAssociatedWithDisorder]->(d:Disorder)
WHERE x <> y
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

query_map = {
    ("gene", "DEFAULT"): default_query,
    ("protein", "DEFAULT"): default_query_protein,
    ("gene", "SHARED_DISORDER"): shared_disorder_query
}

class MustRequest(BaseModel):
    seeds: List[str] = Field(
        None, title="Seed gene(s) for MuST", description="Seed gene(s) for MuST"
    )
    network: str = Field(
        None,
        title="NeDRexDB-based gene-gene network",
        description="NeDRexDB-based gene-gene network. Default: `DEFAULT`",
    )
    hubpenalty: float = Field(
        None,
        title="Hub penalty",
        description="Specific hub penalty between 0.0 and 1.0."
    )
    multiple: bool = Field(
        None,
        title="Multiple",
        description="Boolean flag to indicate whether multiple results should be returned."
    )
    trees: int = Field(
        None,
        title="Trees",
        description="The number of trees to be returned."
    )
    maxit: int = Field(
        None,
        title="Max iterations",
        description="Adjusts the maximum number of iterations to run."
    )


    class Config:
        extra = "forbid"


@router.post("/submit", summary="MuST Submit")
async def must_submit(
    background_tasks: BackgroundTasks, mr: MustRequest = MustRequest()
):
    """
    Submits a job to run MuST using a NEDRexDB-based gene-gene or protein-protein network.

    The required parameters are:
      - `seeds` - a parameter used to identify seed gene(s) or protein(s) for MuST
      - `multiple` - a parameter indicating whether you want multiple results from MuST
      - `maxit` - a parameter used to adjust the maximum number of iterations for MuST
      - `trees` - a parameter used to indicate the number of trees to be returned
    """
    if not mr.seeds:
        raise HTTPException(status_code=404, detail=f"No seed genes submitted")
    if mr.hubpenalty is None:
        raise HTTPException(
            status_code=404, detail=f"Hub penalty not specified"
        )
    if mr.multiple is None:
        raise HTTPException(
            status_code=404, detail=f"Multiple not specified"
        )
    if mr.trees is None:
        raise HTTPException(
            status_code=404, detail=f"Trees not specified"
        )
    if mr.maxit is None:
        raise HTTPException(
            status_code=404, detail=f"Max iterations not specified"
        )

    # Figure out seed type, and strip prefix on submitted seeds
    mr.seeds = [seed.upper() for seed in mr.seeds]

    if all(seed.startswith("ENTREZ.") for seed in mr.seeds):
        seed_type = "gene"
        mr.seeds = [seed.replace("ENTREZ.") for seed in mr.seeds]
    elif all(seed.isnumeric() for seed in mr.seeds):
        seed_type = "gene"
    # Assumption is that if the above aren't met, seeds are proteins.
    elif all(seed.startswith("UNIPROT.") for seed in mr.seeds):
        seed_type = "protein"
        mr.seeds = [seed.replace("UNIPROT.") for seed in mr.seeds]
    else:
        seed_type = "protein"



    query = {
        "seed_genes": sorted(mr.seeds),
        "seed_type": seed_type,
        "network": "DEFAULT" if mr.network is None else mr.network,
        "hub_penalty": mr.hubpenalty,
        "multiple": mr.multiple,
        "trees": mr.trees,
        "maxit": mr.maxit
    }

    result = must_coll.find_one(query)

    if result:
        return result["uid"]

    query["uid"] = f"{uuid.uuid4()}"
    query["status"] = "submitted"

    with DB_LOCK:
        must_coll.insert_one(query)

    background_tasks.add_task(run_must_wrapper, query["uid"])
    return query["uid"]


@router.get("/status", summary="MuST Status")
def must_status(uid: str):
    """
    Returns the details of the MuST job with the given `uid`, including the original query parameters and the status of the job (`submitted`, `running`, `failed`, or `completed`).
    If the job fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = must_coll.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


# @router.get("/download")
# def diamond_download(uid: str):
#     query = {"uid": uid}
#     result = diamond_coll.find_one(query)
#     if not result:
#         raise HTTPException(status_code=404, detail=f"No DIAMOnD job with UID {uid}")
#     if not result["status"] == "completed":
#         raise HTTPException(
#             status_code=404,
#             detail=f"DIAMOnD job with UID {uid} does not have completed status",
#         )
#     return Response(
#         (diamond_dir / (uid + ".txt")).open("rb").read(), media_type="text/plain"
#     )


def run_must_wrapper(uid):
    try:
        run_must(uid)
    except Exception as E:
        print(traceback.format_exc())
        with DB_LOCK:
            must_coll.update_one(
                {"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}}
            )


def run_must(uid):
    with DB_LOCK:
        details = must_coll.find_one({"uid": uid})
        if not details:
            raise Exception()
        must_coll.update_one({"uid": uid}, {"$set": {"status": "running"}})

    # Make a temporary directory to work in
    logger.info("Creating a temporary directory to work in.")
    tempdir = tempfile.TemporaryDirectory()

    # Create the PPI network file
    logger.info("Getting query to generate PPI network.")
    tup = (details["seed_type"], details["network"],)
    query = query_map.get( tup )
    if not query:
        raise Exception()

    prefix = "uniprot." if details["seed_type"] == "protein" else "entrez."

    network_file = get_network(query, prefix)
    shutil.copy(network_file, f"{tempdir.name}/network.tsv")

    logger.info("Writing seed to file.")
    with open(f"{tempdir.name}/seeds.txt", "w") as f:
        for seed in details["seed_genes"]:
            f.write("{}\n".format(seed))

    logger.info("Running DIAMOnD.")
    command = [
        "java",
        "-jar",
        settings.must_run,
        "-hp",
        str(details["hub_penalty"]),
    ]
    if details["multiple"] is True:
        command += ["-m"]
    
    command += ["-mi", f"{details['maxit']}"]
    command += ["-nw", str(network_file)]
    command += ["-s", f"{tempdir.name}/seeds.txt"]
    command += ["-t", f"{details['trees']}"]
    command += ["-oe", f"{must_dir.absolute()}/{details['uid']}_edges.txt"]
    command += ["-on", f"{must_dir.absolute()}/{details['uid']}_nodes.txt"]

    res = subprocess.call(command)
    if res != 0:
        with DB_LOCK:
            must_coll.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"MuST exited with return code {res} -- please check your inputs, and contact API developer if issues persist.",
                    }
                },
            )
        return

    results = {}

    seeds_in_network = set(details["seed_genes"])
    seeds_in_ppi = set()
    with open(f"{tempdir.name}/network.tsv", "r") as f:
        for line in f:
            seeds_in_ppi.update(line.strip().split("\t"))
    seeds_in_network = seeds_in_network.intersection(seeds_in_ppi)

    results["seeds_in_network"] = list(seeds_in_network)
    results["edges"] = []
    results["nodes"] = []
    
    with open(f"{must_dir.absolute()}/{details['uid']}_edges.txt", "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            results["edges"].append(row)

    with open(f"{must_dir.absolute()}/{details['uid']}_nodes.txt", "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            results["nodes"].append(row)   

    tempdir.cleanup()

    with DB_LOCK:
        must_coll.update_one(
            {"uid": uid}, {"$set": {"status": "completed", "results": results}}
        )
    logger.info("Finished MuST.")
