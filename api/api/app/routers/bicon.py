import hashlib
import json
import shutil
import subprocess
import traceback
import os
import uuid
import zipfile

from fastapi import (
    APIRouter,
    BackgroundTasks,
    UploadFile,
    File,
    HTTPException,
    Response,
)

from app.common import DB, DB_LOCK, RESULTS_DIR, get_network, logger, settings

bicon_coll = DB["bicon_"]
bicon_dir = RESULTS_DIR / "bicon_"
if not bicon_dir.exists():
    bicon_dir.mkdir(parents=False, exist_ok=True)

router = APIRouter()

default_query = """
MATCH (pa)-[ppi:ProteinInteractsWithProtein]-(pb)
WHERE "exp" in ppi.evidenceTypes
MATCH (pa)-[:ProteinEncodedBy]->(x)
MATCH (pb)-[:ProteinEncodedBy]->(y)
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

shared_disorder_query = """
MATCH (x:Gene)-[:GeneAssociatedWithDisorder]->(d:Disorder)
MATCH (y:Gene)-[:GeneAssociatedWithDisorder]->(d:Disorder)
WHERE x <> y
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""


@router.post("/submit", summary="BiCoN Submit")
async def bicon_submit(
    background_tasks: BackgroundTasks,
    expression_file: UploadFile = File(...),
    lg_min: int = 10,
    lg_max: int = 15,
    network: str = "DEFAULT",
):
    """
    Route used to submit a BiCoN job.
    BiCoN is an algorithm for network-constrained biclustering of patients and omics data.


    For more information on BiCoN, please see the following publication by Lazareva *et al.*: [BiCoN: Network-constrained biclustering of patients and omics data](https://doi.org/10.1093/bioinformatics/btaa1076)
    """
    uid = f"{uuid.uuid4()}"
    file_obj = expression_file.file
    ext = os.path.splitext(expression_file.filename)[1]

    sha256_hash = hashlib.sha256()
    for byte_block in iter(lambda: file_obj.read(4096), b""):
        sha256_hash.update(byte_block)
    file_obj.seek(0)

    query = {
        "sha256": sha256_hash.hexdigest(),
        "lg_min": lg_min,
        "lg_max": lg_max,
        "network": network,
    }

    with DB_LOCK:
        existing = bicon_coll.find_one(query)
    if existing:
        return existing["uid"]

    upload_dir = bicon_dir / f"{uid}"
    upload_dir.mkdir()
    upload = upload_dir / f"{uid}{ext}"

    query["submitted_filename"] = expression_file.filename
    query["filename"] = upload.name
    query["uid"] = uid
    query["status"] = "submitted"

    print("Uploading file!")
    with upload.open("wb+") as f:
        shutil.copyfileobj(file_obj, f)
    print("Done!")

    with DB_LOCK:
        bicon_coll.insert_one(query)

    background_tasks.add_task(run_bicon_wrapper, uid)

    return uid


@router.get("/status", summary="BiCoN Status")
def bicon_status(uid: str):
    query = {"uid": uid}
    result = bicon_coll.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/clustermap", summary="BiCoN Clustermap")
def bicon_clustermap(uid: str):
    query = {"uid": uid}
    result = bicon_coll.find_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"No BiCoN job with UID {uid}")
    if not result["status"] == "completed":
        raise HTTPException(
            status_code=404,
            detail=f"BiCoN job with UID {uid} does not have completed status",
        )
    with zipfile.ZipFile(bicon_dir / (uid + ".zip"), "r") as f:
        x = f.open(f"{uid}/clustermap.png").read()
    return Response(x, media_type="text/plain")


@router.get("/download", summary="BiCoN Download")
def bicon_download(uid: str):
    query = {"uid": uid}
    result = bicon_coll.find_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"No BiCoN job with UID {uid}")
    if not result["status"] == "completed":
        raise HTTPException(
            status_code=404,
            detail=f"BiCoN job with UID {uid} does not have completed status",
        )
    return Response(
        (bicon_dir / (uid + ".zip")).open("rb").read(), media_type="text/plain"
    )


def run_bicon_wrapper(uid):
    try:
        run_bicon(uid)
    except Exception as E:
        print(traceback.format_exc())
        with DB_LOCK:
            bicon_coll.update_one(
                {"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}}
            )


# NOTE: Input is expected to NOT have the 'entrez.' -- assumed to be Entrez gene IDs.
def run_bicon(uid):
    with DB_LOCK:
        details = bicon_coll.find_one({"uid": uid})
        if not details:
            raise Exception()
        bicon_coll.update_one({"uid": uid}, {"$set": {"status": "running"}})

    workdir = bicon_dir / uid

    if details["network"] == "DEFAULT":
        query = default_query
    elif details["network"] == "SHARED_DISORDER":
        query = shared_disorder_query
    else:
        raise Exception()

    network_file = get_network(query, prefix="entrez.")
    shutil.copy(network_file, f"{workdir / 'network.tsv'}")

    expression = details["filename"]
    lg_max = details["lg_max"]
    lg_min = details["lg_min"]

    command = [
        settings.bicon_env,
        settings.bicon_run,
        "--expression",
        f"{expression}",
        "--network",
        "network.tsv",
        "--lg_min",
        f"{lg_min}",
        "--lg_max",
        f"{lg_max}",
        "--outdir",
        f".",
    ]

    res = subprocess.call(command, cwd=f"{workdir}")
    if res != 0:
        with DB_LOCK:
            bicon_coll.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"BiCoN process exited with exit code {res} -- please check your inputs, and contact API developer if issues persist.",
                    }
                },
            )
        return

    # Load the genes selected, so they can be stored in MongoDB
    result_json = (bicon_dir / uid) / "results.json"
    with result_json.open("r") as f:
        results = json.load(f)
    # Find any edges
    nodes = {i["gene"] for i in results["genes1"] + results["genes2"]}
    edges = set()

    with open(workdir / "network.tsv", "r") as f:
        for line in f:
            a, b = sorted(line.strip().split("\t"))
            if a == b:
                continue
            if a in nodes and b in nodes:
                edges.add((a, b))

    results["edges"] = list(edges)

    # Get patient groups
    *_, patients1, patients2 = (
        open(workdir / "results.csv").read().strip().split("\n")[1].split(",")
    )
    results["patients1"] = patients1.split("|")
    results["patients2"] = patients2.split("|")

    command = ["zip", "-r", "-D", f"{uid}.zip", f"{uid}"]

    res = subprocess.call(command, cwd=f"{bicon_dir}")
    if res != 0:
        with DB_LOCK:
            bicon_coll.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"Attempt to zip results exited with return code {res} -- please contact API developer if issues persist.",
                    }
                },
            )
        return

    shutil.rmtree(f"{bicon_dir / uid}")
    with DB_LOCK:
        bicon_coll.update_one(
            {"uid": uid}, {"$set": {"status": "completed", "result": results}}
        )
