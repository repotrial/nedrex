import csv
import shutil
import subprocess
import tempfile
import time
import traceback
import uuid
from itertools import combinations, product
from typing import List

from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from pydantic import BaseModel, Field

from app.common import DB_LOCK, DB, RESULTS_DIR, get_network, logger, settings

router = APIRouter()

diamond_coll = DB["diamond_"]
diamond_dir = RESULTS_DIR / "diamond_"
if not diamond_dir.exists():
    diamond_dir.mkdir(parents=False, exist_ok=True)

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

class DiamondRequest(BaseModel):
    seeds: List[str] = Field(
        None, title="Seed gene(s) for DIAMOnD", description="Seed gene(s) for DIAMOnD"
    )
    n: int = Field(
        None,
        title="The maximum number of nodes (genes) at which to stop the algorithm",
        description="The maximum number of nodes (genes) at which to stop the algorithm",
    )
    alpha: int = Field(
        None,
        title="Weight given to seeds",
        description="Weight given to seeds. Default: 1",
    )
    network: str = Field(
        None,
        title="NeDRexDB-based gene-gene network",
        description="NeDRexDB-based gene-gene network. Default: `DEFAULT`",
    )
    edges: str = Field(
        None,
        title="Edges to return in the results",
        description="Option affecting which edges are returned in the results. "
            "Options are `all`, which returns edges in the PPI between nodes in the DIAMOnD module and `limited`, which only returns edges between seeds and new nodes. "
            "Default: `all`",
    )

    class Config:
        extra = "forbid"


@router.post("/submit", summary="DIAMOnD Submit")
async def diamond_submit(
    background_tasks: BackgroundTasks, dr: DiamondRequest = DiamondRequest()
):
    """
    Submits a job to run DIAMOnD using a NeDRexDB-based gene-gene network.

    The required parameters are:
      - `seeds` - a parameter used to identify seed gene(s) for DIAMOnD
      - `n` - a parameter indiciating the maximum number of nodes (genes) at which to stop the algorithm
      - `alpha` - a parameter used to give weight to the seeds
      - `network` - a parameter used to identify the NeDRexDB-based gene-gene network to use

    At present, two values are supported for `network` -- `DEFAULT`, where two genes are linked if they encode proteins with an
    experimentally asserted PPI, and `SHARED_DISORDER`, where two genes are linked if they are both asserted to be involved in the same disorder.

    Seeds, `seeds`, should be Entrez gene IDs (without any database as part of the identifier -- i.e., `2717`, not `entrez.2717`).
    An example route is [/diamond/submit?s=2717&s=5836&s=2760&n=100](https://api.nedrex.net/diamond/submit?s=2717&s=5836&s=2760&n=100).
    Note, clicking the above link will not work, as the request needs to be submitted as POST, not GET.

    A successfully submitted request will return a UID which can be used in other routes to (1) check the status of the DIAMOnD run and (2) download the results.

    For more information on DIAMOnD, please see the following paper by Ghiassian *et al.*: [A DIseAse MOdule Detection (DIAMOnD) Algorithm Derived from a Systematic Analysis of Connectivity Patterns of Disease Proteins in the Human Interactome](https://doi.org/10.1371/journal.pcbi.1004120)

    """
    if not dr.seeds:
        raise HTTPException(status_code=404, detail=f"No seed genes submitted")
    if not dr.n:
        raise HTTPException(
            status_code=404, detail=f"Number of genes returned not specified"
        )

    # Figure out seed type, and strip prefix on submitted seeds
    dr.seeds = [seed.upper() for seed in dr.seeds]

    if all(seed.startswith("ENTREZ.") for seed in dr.seeds):
        seed_type = "gene"
        dr.seeds = [seed.replace("ENTREZ.") for seed in dr.seeds]
    elif all(seed.isnumeric() for seed in dr.seeds):
        seed_type = "gene"
    # Assumption is that if the above aren't met, seeds are proteins.
    elif all(seed.startswith("UNIPROT.") for seed in dr.seeds):
        seed_type = "protein"
        dr.seeds = [seed.replace("UNIPROT.") for seed in dr.seeds]
    else:
        seed_type = "protein"

    if dr.edges is None:
        dr.edges = "all"
    if dr.edges not in {"all", "limited"}:
        raise HTTPException(
            status_code=404, detail=f"If specified, edges must be `limited` or `all`"
        )

    query = {
        "seed_genes": sorted(dr.seeds),
        "seed_type": seed_type,
        "n": dr.n,
        "alpha": 1 if dr.alpha is None else dr.alpha,
        "network": "DEFAULT" if dr.network is None else dr.network,
        "edges": dr.edges
    }

    result = diamond_coll.find_one(query)

    if result:
        return result["uid"]

    query["uid"] = f"{uuid.uuid4()}"
    query["status"] = "submitted"

    with DB_LOCK:
        diamond_coll.insert_one(query)

    background_tasks.add_task(run_diamond_wrapper, query["uid"])
    return query["uid"]


@router.get("/status", summary="DIAMOnD Status")
def diamond_status(uid: str):
    """
    Returns the details of the DIAMOnD job with the given `uid`, including the original query parameters and the status of the build (`submitted`, `building`, `failed`, or `completed`).
    If the build fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = diamond_coll.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/download", summary="DIAMOnD Download")
def diamond_download(uid: str):
    query = {"uid": uid}
    result = diamond_coll.find_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"No DIAMOnD job with UID {uid}")
    if not result["status"] == "completed":
        raise HTTPException(
            status_code=404,
            detail=f"DIAMOnD job with UID {uid} does not have completed status",
        )
    return Response(
        (diamond_dir / (uid + ".txt")).open("rb").read(), media_type="text/plain"
    )


def run_diamond_wrapper(uid):
    try:
        run_diamond(uid)
    except Exception as E:
        print(traceback.format_exc())
        with DB_LOCK:
            diamond_coll.update_one(
                {"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}}
            )


def run_diamond(uid):
    with DB_LOCK:
        details = diamond_coll.find_one({"uid": uid})
        if not details:
            raise Exception()
        diamond_coll.update_one({"uid": uid}, {"$set": {"status": "running"}})

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
        settings.diamond_run,
        "--network_file",
        f"{tempdir.name}/network.tsv",
        "--seed_file",
        f"{tempdir.name}/seeds.txt",
        "-n",
        f"{details['n']}",
        "--alpha",
        f"{details['alpha']}",
        "-o",
        f"{tempdir.name}/results.txt",
    ]

    res = subprocess.call(command)
    if res != 0:
        with DB_LOCK:
            diamond_coll.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"DIAMOnD exited with return code {res} -- please check your inputs, and contact API developer if issues persist.",
                    }
                },
            )
        return

    # Extract results
    logger.info("Extracting results")
    results = {"diamond_nodes": [], "edges": []}
    diamond_nodes = set()

    with open(f"{tempdir.name}/results.txt", "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            row = dict(row)
            row["rank"] = row.pop("#rank")
            results["diamond_nodes"].append(row)
            diamond_nodes.add(row["DIAMOnD_node"])

    seeds = set(details["seed_genes"])
    seeds_in_network = set()

    # Get edges between DIAMOnD results and seeds
    logger.info("Getting edges between DIAMOnD results and seeds.")

    if details["edges"] == "all":
        module_nodes = set(diamond_nodes) | set(seeds)
        possible_edges = {tuple(sorted(i)) for i in combinations(module_nodes, 2)}

        with open(f"{tempdir.name}/network.tsv") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                sorted_row = tuple(sorted(row))
                if sorted_row in possible_edges:
                    results["edges"].append(sorted_row)

                for node in sorted_row:
                    if node in seeds:
                        seeds_in_network.add(node)

    elif details["edges"] == "limited":
        possible_edges = {tuple(sorted(i)) for i in product(diamond_nodes, seeds)}

        with open(f"{tempdir.name}/network.tsv") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                sorted_row = tuple(sorted(row))
                if sorted_row in possible_edges:
                    results["edges"].append(sorted_row)

                for node in sorted_row:
                    if node in seeds:
                        seeds_in_network.add(node)

    # Remove duplicates
    results["edges"] = {tuple(i) for i in results["edges"]}
    results["edges"] = [list(i) for i in results["edges"]]


    results["seeds_in_network"] = sorted(seeds_in_network)

    shutil.move(f"{tempdir.name}/results.txt", diamond_dir / f"{details['uid']}.txt")

    tempdir.cleanup()

    with DB_LOCK:
        diamond_coll.update_one(
            {"uid": uid}, {"$set": {"status": "completed", "results": results}}
        )
    logger.info("Finished DIAMOnD.")
