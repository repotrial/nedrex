import json
import uuid
from collections import defaultdict
from collections.abc import MutableMapping
from itertools import chain
from typing import List

import networkx as nx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from pydantic import BaseModel, Field
from typing_extensions import Final

from app.common import (
    DB,
    DB_LOCK,
    RESULTS_DIR,
    NODE_COLLECTIONS,
    EDGE_COLLECTIONS,
    STATIC_DIR,
    logger,
)

router = APIRouter()
graph_coll = DB["graphs_"]
graph_dir = RESULTS_DIR / "graphs_"
if not graph_dir.exists():
    graph_dir.mkdir(parents=False, exist_ok=True)

DEFAULT_NODE_COLLECTIONS: Final = ["disorder", "drug", "gene", "protein"]
DEFAULT_EDGE_COLLECTIONS: Final = [
    "disorder_is_subtype_of_disorder",
    "drug_has_indication",
    "drug_has_target",
    "gene_associated_with_disorder",
    "protein_encoded_by",
    "protein_interacts_with_protein",
    "disorder_comorbid_with_disorder",
]

NODE_TYPE_MAP: Final = {
    "disorder": ["Disorder"],
    "drug": ["Drug", "BiotechDrug", "SmallMoleculeDrug"],
    "gene": ["Gene"],
    "pathway": ["Pathway"],
    "protein": ["Protein"],
    "signature": ["Signature"]
}


# Helper function to flatten dictionaries
def flatten(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    rtrn = {}
    for k, v in items:
        if isinstance(v, list):
            rtrn[k] = ", ".join(v)
        elif v is None:
            rtrn[k] = "None"
        else:
            rtrn[k] = v

    return rtrn


def check_values(supplied, valid, property_name):
    invalid = [i for i in supplied if i not in valid]
    if invalid:
        raise HTTPException(
            status_code=404, detail=f"Invalid value(s) for {property_name}: {invalid!r}"
        )


class BuildRequest(BaseModel):
    nodes: List[str] = Field(
        None,
        title="Node types to include in the graph",
        description=f"Default: `['disorder', 'drug', 'gene', 'protein']`",
    )
    edges: List[str] = Field(
        None,
        title="Edge types to include in the graph",
        description=f"Default: `['disorder_is_subtype_of_disorder', 'drug_has_indication', 'drug_has_target', 'gene_associated_with_disorder', 'protein_encoded_by', 'protein_interacts_with_protein']`",
    )
    iid_evidence: List[str] = Field(
        None, title="IID evidence types", description="Default: `['exp']`"
    )
    ppi_self_loops: bool = Field(
        None,
        title="PPI self-loops",
        description="Filter on in/ex-cluding PPI self-loops (default: `False`)",
    )
    taxid: List[int] = Field(
        None,
        title="Taxonomy IDs",
        description="Filters proteins by TaxIDs (default: `[9606]`)",
    )
    drug_groups: List[str] = Field(
        None,
        title="Drug groups",
        description="Filters drugs by drug groups (default: `['approved']`",
    )
    concise: bool = Field(
        None,
        title="Concise",
        description="Setting the concise flag to `True` will only give nodes a primaryDomainId and type, and edges a type. Default: `True`",
    )
    include_omim: bool = Field(
        None,
        title="Include OMIM gene-disorder associations",
        description="Setting the include_omim flag to `True` will include gene-disorder associations from OMIM. Default: `True`",
    )
    disgenet_threshold: float = Field(
        None,
        title="DisGeNET threshold",
        description="Threshold for gene-disorder associations from DisGeNET. Default: `0` (gives all assocations)",
    )
    use_omim_ids: bool = Field(
        None,
        title="Prefer OMIM IDs on disorders",
        description="Replaces the primaryDomainId on disorder nodes with an OMIM ID where an unambiguous OMIM ID exists. Default: `False`",
    )
    split_drug_types: bool = Field(
        None,
        title="Split drugs into subtypes",
        description="Replaces type on Drugs with BiotechDrug or SmallMoleculeDrug as appropriate. Default: `False`",
    )

    class Config:
        extra = "forbid"


@router.post(
    "/graph_builder",
    responses={
        200: {
            "content": {
                "application/json": {"example": "d961c377-cbb3-417f-a4b0-cc1996ce6f51"}
            }
        },
        404: {
            "content": {
                "application/json": {
                    "example": {"detail": "Invalid values for n: ['tissue']"}
                }
            }
        },
    },
    summary="Graph builder",
)
async def graph_builder(
    background_tasks: BackgroundTasks, build_request: BuildRequest = BuildRequest()
):
    """
    Returns the UID for the graph build with user-given parameters, and additionally sets a build running if
    the build does not exist. The graph is built according to the following rules:

    * Nodes are added first, with proteins only added if the taxid recorded is in `taxid` query value, and drugs only added if the drug group is in the `drug_group` query value.

    * Edges are then added, with an edge only added if the nodes it connets are both in the database. Additionally, protein-protein interactions (PPIs) can be filtered by IID evidence type using the `?iid_evidence` query parameter. By default, self-loop PPIs are not added, but this can be changed by setting the `ppi_self_loops` query value to `true`.

    Acceptable values for `nodes` and `edges` can be seen by querying [/list_node_collections](https://api.nedrex.net/list_node_collections) and [/list_edge_collections](https://api.nedrex.net/list_edge_collections) respectively. For the remaining query parameters, acceptable values are as follows:

        // 9606 is Homo sapiens, -1 is used for "not recorded in NeDRexDB".
        taxid = [-1, 9606]
        // Default is just approved.
        drug_group = ['approved', 'experimental', 'illicit', 'investigational', 'nutraceutical', 'vet_approved', 'withdrawn']
        // exp = experimental, pred = predicted, orth = orthology
        iid_evidence = ['exp', 'ortho', 'pred']
    """
    valid_taxid = [-1, 9606]
    valid_drug_groups = [
        "approved",
        "experimental",
        "illicit",
        "investigational",
        "nutraceutical",
        "vet_approved",
        "withdrawn",
    ]
    valid_iid_evidence = ["exp", "ortho", "pred"]

    if build_request.nodes is None:
        build_request.nodes = sorted(DEFAULT_NODE_COLLECTIONS)
    check_values(build_request.nodes, NODE_COLLECTIONS, "nodes")

    if build_request.edges is None:
        build_request.edges = sorted(DEFAULT_EDGE_COLLECTIONS)
    check_values(build_request.edges, EDGE_COLLECTIONS, "edges")

    if build_request.iid_evidence is None:
        build_request.iid_evidence = ["exp"]
    check_values(build_request.iid_evidence, valid_iid_evidence, "iid_evidence")

    if build_request.ppi_self_loops is None:
        build_request.ppi_self_loops = False

    if build_request.taxid is None:
        build_request.taxid = [9606]
    check_values(build_request.taxid, valid_taxid, "taxid")

    if build_request.drug_groups is None:
        build_request.drug_groups = ["approved"]
    check_values(build_request.drug_groups, valid_drug_groups, "drug_groups")

    if build_request.include_omim is None:
        build_request.include_omim = True

    if build_request.disgenet_threshold is None:
        build_request.disgenet_threshold = 0
    elif build_request.disgenet_threshold < 0:
        build_request.disgenet_threshold = -1
    elif build_request.disgenet_threshold > 1:
        build_request.disgenet_threshold = 2.0

    if build_request.concise is None:
        build_request.concise = True

    if build_request.use_omim_ids is None:
        build_request.use_omim_ids = False

    if build_request.split_drug_types is None:
        build_request.split_drug_types = False

    query = dict(build_request)

    with (STATIC_DIR / "metadata.json").open() as f:
        query["version"] = json.load(f)["version"]

    with DB_LOCK:
        result = graph_coll.find_one(query)
        if not result:
            query["status"] = "submitted"
            query["uid"] = f"{uuid.uuid4()}"
            graph_coll.insert_one(query)
            uid = query["uid"]
            background_tasks.add_task(graph_constructor_wrapper, query)
        else:
            uid = result["uid"]

    return uid


@router.get(
    "/graph_details/{uid}",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "nodes": ["disorder", "drug", "gene", "pathway", "protein"],
                        "edges": [
                            "disorder_comorbid_with_disorder",
                            "disorder_is_subtype_of_disorder",
                            "drug_has_indication",
                            "drug_has_target",
                            "gene_associated_with_disorder",
                            "is_isoform_of",
                            "molecule_similarity_molecule",
                            "protein_encoded_by",
                            "protein_in_pathway",
                            "protein_interacts_with_protein",
                        ],
                        "iid_evidence": ["exp"],
                        "ppi_self_loops": False,
                        "taxid": [9606],
                        "drug_groups": ["approved"],
                        "status": "completed",
                        "uid": "d961c377-cbb3-417f-a4b0-cc1996ce6f51",
                    }
                }
            }
        }
    },
    summary="Graph details",
)
def graph_details(uid: str):
    """
    Returns the details of the graph with the given UID,
    including the original query parameters and the status of the build (`submitted`, `building`, `failed`, or `completed`).
    If the build fails, then these details will contain the error message.
    """
    data = graph_coll.find_one({"uid": uid})

    if data:
        data.pop("_id")
        return data

    raise HTTPException(
        status_code=404, detail=f"No graph with UID {uid!r} is recorded."
    )


@router.get("/graph_download/{uid}.graphml", summary="Graph download")
def graph_download(uid: str):
    """
    Returns the graph with the given `uid` in GraphML format.
    """
    data = graph_coll.find_one({"uid": uid})

    if data and data["status"] == "completed":
        return Response(
            (graph_dir / f"{uid}.graphml").open("r").read(), media_type="text/plain"
        )
    elif data and data["status"] != "completed":
        raise HTTPException(
            status_code=404,
            detail=f"Graph with UID {uid!r} does not have completed status.",
        )
    # If data doesn't exist, means that the graph with the UID supplied does not exist.
    elif not data:
        raise HTTPException(
            status_code=404, detail=f"No graph with UID {uid!r} is recorded."
        )


@router.get("/graph_download_v2/{uid}/{fname}.graphml", summary="Graph download")
def graph_download_ii(fname: str, uid: str):
    """
    Returns the graph with the given `uid` in GraphML format.
    The `fname` path parameter can be anything a user desires, and is used simply to allow a user to download the graph with their desired filename.
    """
    data = graph_coll.find_one({"uid": uid})

    if data and data["status"] == "completed":
        return Response(
            (graph_dir / f"{uid}.graphml").open("r").read(), media_type="text/plain"
        )

    elif data and data["status"] != "completed":
        raise HTTPException(
            status_code=404,
            detail=f"Graph with UID {uid!r} does not have completed status.",
        )
    # If data doesn't exist, means that the graph with the UID supplied does not exist.
    elif not data:
        raise HTTPException(
            status_code=404, detail=f"No graph with UID {uid!r} is recorded."
        )


def graph_constructor_wrapper(query):
    try:
        graph_constructor(query)
    except Exception as E:
        with DB_LOCK:
            graph_coll.update_one(
                {"uid": query["uid"]}, {"$set": {"status": "failed", "error": f"{E}"}}
            )


def graph_constructor(query):
    with DB_LOCK:
        graph_coll.update_one({"uid": query["uid"]}, {"$set": {"status": "building"}})

    G = nx.DiGraph()

    logger.debug("Adding edges")

    for coll in query["edges"]:
        print(f"\tAdding edge collection: {coll}")

        # Apply filters (if given) on PPI edges.
        if coll == "protein_interacts_with_protein":
            cursor = DB[coll].find({"evidenceTypes": {"$in": query["iid_evidence"]}})

            # If this is true, then we keep PPI self-loops.
            for doc in cursor:
                m1 = doc["memberOne"]
                m2 = doc["memberTwo"]

                if not query["ppi_self_loops"] and (m1 == m2):
                    continue
                if query["concise"]:
                    G.add_edge(
                        m1,
                        m2,
                        memberOne=m1,
                        memberTwo=m2,
                        reversible=True,
                        type=doc["type"],
                        evidenceTypes=", ".join(doc["evidenceTypes"]),
                    )
                else:
                    doc.pop("_id")
                    G.add_edge(m1, m2, reversible=True, **flatten(doc))
            continue

        # Apply filters on gene-disorder edges.
        if coll == "gene_associated_with_disorder":
            if query["include_omim"]:
                c1 = DB[coll].find({"assertedBy": "omim"})
            else:
                c1 = []

            c2 = DB[coll].find({"score": {"$gte": query["disgenet_threshold"]}})

            for doc in chain(c1, c2):
                s = doc["sourceDomainId"]
                t = doc["targetDomainId"]

                # There is no difference in attributes between concise and non-concise.
                # If / else in just to show that there is no difference.
                if query["concise"]:
                    doc.pop("_id")
                    G.add_edge(s, t, reversible=False, **flatten(doc))
                else:
                    doc.pop("_id")
                    G.add_edge(s, t, reversible=False, **flatten(doc))
            continue

        cursor = DB[coll].find()
        for doc in cursor:
            # Check for memberOne/memberTwo syntax (undirected).
            if ("memberOne" in doc) and ("memberTwo" in doc):
                m1 = doc["memberOne"]
                m2 = doc["memberTwo"]
                if query["concise"]:
                    G.add_edge(
                        m1,
                        m2,
                        reversible=True,
                        type=doc["type"],
                        memberOne=m1,
                        memberTwo=m2,
                    )
                else:
                    doc.pop("_id")
                    G.add_edge(m1, m2, reversible=True, **flatten(doc))

            # Check for source/target syntax (directed).
            elif ("sourceDomainId" in doc) and ("targetDomainId" in doc):
                s = doc["sourceDomainId"]
                t = doc["targetDomainId"]

                if query["concise"]:
                    G.add_edge(
                        s,
                        t,
                        reversible=False,
                        sourceDomainId=s,
                        targetDomainId=t,
                        type=doc["type"],
                    )
                else:
                    doc.pop("_id")
                    G.add_edge(s, t, reversible=False, **flatten(doc))

            else:
                raise Exception("Assumption about edge structure violated.")

    logger.debug("Adding nodes")
    for coll in query["nodes"]:
        # Apply the taxid filter to protein.
        if coll == "protein":
            cursor = DB[coll].find({"taxid": {"$in": query["taxid"]}})
        # Apply the drug groups filter to drugs.
        elif coll == "drug":
            cursor = DB[coll].find({"drugGroups": {"$in": query["drug_groups"]}})
        else:
            cursor = DB[coll].find()

        for doc in cursor:
            node_id = doc["primaryDomainId"]
            G.add_node(node_id, primaryDomainId=node_id)

    logger.debug("Removing nodes not matching query")

    cursor = DB["protein"].find({"taxid": {"$not": {"$in": query["taxid"]}}})
    ids = [i["primaryDomainId"] for i in cursor]
    G.remove_nodes_from(ids)

    cursor = DB["drug"].find({"drugGroups": {"$not": {"$in": query["drug_groups"]}}})
    ids = [i["primaryDomainId"] for i in cursor]
    G.remove_nodes_from(ids)

    ############################################
    # ADD ATTRIBUTES
    ############################################

    # Problem:
    #  We don't know what types the nodes are.

    # Solution:
    # Iterate over all collections (quick), see if the node / edge is in the graph (quick), and decorate with attributes
    logger.debug("Adding node attributes")

    updates = {}
    node_ids = set(G.nodes())

    for node in NODE_COLLECTIONS:
        cursor = DB[node].find()
        for doc in cursor:
            eid = doc["primaryDomainId"]
            if eid not in node_ids:
                continue

            if node == "drug":
                doc["drugClass"] = doc["_cls"].split(".")[1]
                del doc["_cls"]

            if node == "drug" and query["split_drug_types"] is False:
                doc["type"] = "Drug"

            if query["concise"]:
                assert eid not in updates

                if doc["type"] == "Pathway":
                    attrs = ["primaryDomainId", "displayName", "type"]
                elif doc["type"] == "Drug":
                    attrs = [
                        "primaryDomainId",
                        "domainIds",
                        "displayName",
                        "synonyms",
                        "type",
                        "drugGroups",
                        "indication",
                    ]
                elif doc["type"] == "Disorder":
                    attrs = [
                        "primaryDomainId",
                        "domainIds",
                        "displayName",
                        "synonyms",
                        "icd10",
                        "type",
                    ]
                elif doc["type"] == "Gene":
                    attrs = [
                        "primaryDomainId",
                        "displayName",
                        "synonyms",
                        "approvedSymbol",
                        "symbols",
                        "type",
                    ]
                elif doc["type"] == "Protein":
                    attrs = [
                        "primaryDomainId",
                        "displayName",
                        "geneName",
                        "taxid",
                        "type",
                    ]
                elif doc["type"] == "Signature":
                    attrs = ["primaryDomainId"]
                else:
                    raise ("Exception!")

                doc = {attr: doc.get(attr, "") for attr in attrs}
                updates[eid] = flatten(doc)

            else:
                assert eid not in updates
                doc.pop("_id")
                updates[eid] = flatten(doc)

    nx.set_node_attributes(G, updates)

    ############################################
    # SORTING LONE NODES
    ############################################
    nodes_requested = set(chain(*[NODE_TYPE_MAP[coll] for coll in query["nodes"]]))
    to_remove = set()

    for node, data in G.nodes(data=True):
        # If the type of the node is one of the requested types, do nothing.
        if data["type"] in nodes_requested:
            continue
        # Otherwise, check the node is involved in at least one edge.
        elif G.in_edges(node) or G.out_edges(node):
            continue
        else:
            to_remove.add(node)

    G.remove_nodes_from(to_remove)


    ############################################
    # CUSTOM CHANGES
    ############################################

    if query["use_omim_ids"]:
        # We need nodes with unambiguous OMIM IDs.
        mondomim_map = defaultdict(list)
        for doc in DB["disorder"].find():
            omim_xrefs = [i for i in doc["domainIds"] if i.startswith("omim.")]
            if len(omim_xrefs) == 1:
                mondomim_map[omim_xrefs[0]].append(doc["primaryDomainId"])

        mondomim_map = {
            v[0]: k
            for k, v in mondomim_map.items()
            if (len(v) == 1) and v[0] in G.nodes
        }

        nx.set_node_attributes(
            G, {k: {"primaryDomainId": v} for k, v in mondomim_map.items()}
        )
        G = nx.relabel_nodes(G, mondomim_map)
        updates = defaultdict(dict)
        for i, j, data in G.edges(data=True):
            if "memberOne" in data and data["memberOne"] != i:
                updates[(i, j)]["memberOne"] = i
            if "memberTwo" in data and data["memberTwo"] != j:
                updates[(i, j)]["memberTwo"] = j
            if "sourceDomainId" in data and data["sourceDomainId"] != i:
                updates[(i, j)]["sourceDomainId"] = i
            if "targetDomainId" in data and data["targetDomainId"] != j:
                updates[(i, j)]["targetDomainId"] = j

        nx.set_edge_attributes(G, updates)

    logger.debug("Finished building!")

    nx.write_graphml(G, f"{graph_dir / query['uid']}.graphml")
    with DB_LOCK:
        graph_coll.update_one(
            {"uid": query["uid"]}, {"$set": {"status": "completed"}}
        )
