import json
from fastapi import APIRouter, Response

from app.common import STATIC_DIR

router = APIRouter()

@router.get("/metadata", summary="Metadata and versions of source datasets for the NeDRex database")
def get_metadata():
    """
    Returns a JSON object containing the current NeDRexDB version used by the API, and the date the source
    databases contributing to the database were obtained.
    """
    p = STATIC_DIR / "metadata.json"
    with p.open() as f:
        metadata = json.load(f)
    return metadata

@router.get("/msigdb_set/{msigdb_id}", summary="Get MSigDB gene set")
def get_msigdb_gene_set(msigdb_id: str):
    """
    Return the gene IDs of genes in the given MSigDB set.
    An example route for this is [/static/msigdb_set/KAUFFMANN_DNA_REPAIR_GENES](https://api.nedrex.net/static/msigdb_set/KAUFFMANN_DNA_REPAIR_GENES).

    For a full list of possible sets, see the [list MSigDB gene sets](https://api.nedrex.net/#operation/list_msigdb_gene_sets_static_msigdb_sets_get) route.
    """
    p = STATIC_DIR / "msigdb.v7.1.entrez.gmt"
    for line in p.open():
        msid, _, *genes = line.strip().split("\t")
        if msid == msigdb_id:
            return [f"entrez.{gene_id}" for gene_id in genes]


@router.get("/msigdb_sets", summary="List MSigDB gene sets")
def list_msigdb_gene_sets():
    """
    Returns an array of MSigDB set IDs.
    """
    p = STATIC_DIR / "msigdb.v7.1.entrez.gmt"
    ids = []
    for line in p.open():
        msid, *_ = line.strip().split("\t")
        ids.append(msid)

    return ids


@router.get("/harmonizome/{mondo_id}", summary="Get harmonizome gene sets by MONDO ID")
def get_harmonizome_gene_set(mondo_id: str):
    """
    Return the gene IDs of genes in Harmonizome gene sets associated with the MONDO ID.
    For example, the gene IDs of genes in Harmonizome gene sets associated with generalized anxiety disorder can be obtained at [/static/harmonizome/mondo.0001942](https://api.nedrex.net/static/harmonizome/mondo.0001942).
    """
    p = STATIC_DIR / "harmonizome.json"
    data = json.load(p.open())
    return data.get(mondo_id)


@router.get("/lengths.map", summary="Lengths map")
def lengths_map_download():
    """
    Returns the lengths.map file, required for some functions in the NeDRex platform.
    """
    return Response(
        (STATIC_DIR / "lengths.map").open("r").read(), media_type="text/plain"
    )
