from csv import DictWriter
from io import StringIO
from typing import List, Set

from cachetools import LRUCache, cached
from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel, Field

from app.common import NODE_COLLECTIONS, EDGE_COLLECTIONS, DB

router = APIRouter()


@router.get(
    "/list_node_collections",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": ["disorder", "drug", "gene", "pathway", "protein"]
                }
            }
        }
    },
    summary="List node collections",
)
def list_node_collections():
    """
    Returns an array of the node collections (types) available in the NeDRexDB.
    """
    return NODE_COLLECTIONS


@router.get(
    "/list_edge_collections",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": [
                        "disorder_is_subtype_of_disorder",
                        "drug_has_contraindication",
                        "drug_has_indication",
                        "drug_has_target",
                        "gene_associated_with_disorder",
                        "is_isoform_of",
                        "molecule_similarity_molecule",
                        "protein_encoded_by",
                        "protein_has_signature",
                        "protein_in_pathway",
                        "protein_interacts_with_protein",
                        "protein_similarity_protein"
                    ]
                }
            }
        }
    },
    summary="List edge collections",
)
def list_edge_collections():
    """
    Returns an array of the edge collections (types) available in the NeDRexDB.
    """
    return EDGE_COLLECTIONS


class AttributeRequest(BaseModel):
    node_ids: List[str] = Field(None, title="Primary domain IDs of nodes")
    attributes: List[str] = Field(None, title="Attributes requested")

    class Config:
        extra = "forbid"

@router.get(
    "/{t}/attributes",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": [
                        "synonyms",
                        "domainIds",
                        "primaryDomainId",
                        "type",
                        "displayName",
                        "comments",
                        "taxid",
                        "sequence",
                        "geneName",
                    ]
                }
            }
        },
        404: {
            "content": {
                "application/json": {
                    "example": {"detail": "Collection 'tissue' is not in the database"}
                }
            }
        },
    },
    summary="List collection attributes",
)
@cached(cache=LRUCache(maxsize=32))
def list_attributes(t: str):
    """
    Returns an array of the attributes that items in collection `t` have. For example, the route [/gene/attributes](https://api.nedrex.net/gene/attributes) 
    returns the attributes that entities in the gene collection have.
    """
    if t not in NODE_COLLECTIONS + EDGE_COLLECTIONS:
        raise HTTPException(
            status_code=404, detail=f"Collection {t!r} is not in the database"
        )
    attributes: Set[str] = set()
    for doc in DB[t].find():
        attributes = attributes | set(doc.keys())
    attributes.remove("_id")
    return attributes

@router.get(
    "/{t}/attributes/{attribute}/{format}",
    summary="Get collection member attribute values",
)
def get_attribute_values(t: str, attribute: str, format: str):
    """
    Get `attribute` values for entities of type `t`. Data is returned in `format`. Example routes are:

    * `/protein/attributes/displayName/tsv`, which will return a map (in TSV format) of primary domain IDs to display names for entities of the protein type.
    * `/drug/attributes/drugGroups/json`, which will return a map (in JSON format) of primary domain IDs to drug groups for entities of the drug type.

    To find out what collections (entities) are in the database, see the [list node collections](https://api.nedrex.net/#operation/list_node_collections_list_node_collections_get)
    and [list edge collections](https://api.nedrex.net/#operation/list_edge_collections_list_edge_collections_get) routes. To find out what attributes
    a given collection has, see the [list collection attributes](https://api.nedrex.net/#operation/list_attributes__t__attributes_get) route.
    """
    if t in NODE_COLLECTIONS:
        results = [
            {"primaryDomainId": i["primaryDomainId"], attribute: i.get(attribute)}
            for i in DB[t].find()
        ]
    elif t in EDGE_COLLECTIONS:
        try:
            results = [
                {
                    "sourceDomainId": i["sourceDomainId"],
                    "targetDomainId": i["targetDomainId"],
                    attribute: i.get(attribute),
                }
                for i in DB[t].find()
            ]
        except KeyError:
            results = [
                {
                    "memberOne": i["memberOne"],
                    "memberTwo": i["memberTwo"],
                    attribute: i.get(attribute),
                }
                for i in DB[t].find()
            ]
    else:
        raise HTTPException(
            status_code=404, detail=f"Collection {t!r} is not in the database"
        )

    if format == "json":
        return results

    elif format == "csv":
        string = StringIO()
        keys = results[0].keys()
        dict_writer = DictWriter(string, keys, delimiter=",")
        dict_writer.writeheader()
        dict_writer.writerows(results)
        return Response(content=string.getvalue(), media_type="plain/text")

    elif format == "tsv":
        string = StringIO()
        keys = results[0].keys()
        dict_writer = DictWriter(string, keys, delimiter="\t")
        dict_writer.writeheader()
        dict_writer.writerows(results)
        return Response(content=string.getvalue(), media_type="plain/text")


@router.get(
    "/{t}/attributes_v2/{format}",
    summary="Get collection member attribute values (version 2)",
)
def get_node_attribute_values_v2(
    t: str, format: str, ar: AttributeRequest = AttributeRequest()
):
    """
    Similar to the "get collection member attribute values" route, this route returns `attribute` values for entities of type `t`. 
    Data is returned in the specified `format` (CSV, TSV, or JSON). 
    However, this route allows you to specify an array of `node_ids` in the request body, allowing a subset of nodes to be requested.

    To find out what collections (entities) are in the database, see the [list node collections](https://api.nedrex.net/#operation/list_node_collections_list_node_collections_get)
    and [list edge collections](https://api.nedrex.net/#operation/list_edge_collections_list_edge_collections_get) routes. To find out what attributes
    a given collection has, see the [list collection attributes](https://api.nedrex.net/#operation/list_attributes__t__attributes_get) route.
    """
    if t not in NODE_COLLECTIONS:
        raise HTTPException(
            status_code=404, detail=f"Collection {t!r} is not in the database"
        )

    if ar.attributes is None:
        raise HTTPException(status_code=404, detail=f"No attribute(s) requested")
    if ar.node_ids is None:
        raise HTTPException(status_code=404, detail=f"No node(s) requested")

    query = {"primaryDomainId": {"$in": ar.node_ids}}

    results = [
        {
            "primaryDomainId": i["primaryDomainId"],
            **{attribute: i.get(attribute) for attribute in ar.attributes},
        }
        for i in DB[t].find(query)
    ]

    if format == "json":
        return results

    elif format == "csv":
        string = StringIO()
        keys = results[0].keys()
        dict_writer = DictWriter(string, keys, delimiter=",")
        dict_writer.writeheader()
        dict_writer.writerows(results)
        return Response(content=string.getvalue(), media_type="plain/text")

    elif format == "tsv":
        string = StringIO()
        keys = results[0].keys()
        dict_writer = DictWriter(string, keys, delimiter="\t")
        dict_writer.writeheader()
        dict_writer.writerows(results)
        return Response(content=string.getvalue(), media_type="plain/text")


@router.get(
    "/{t}/details",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "ns": "test.drug",
                        "size": 16029934,
                        "count": 13300,
                        "avgObjSize": 1205,
                        "storageSize": 8798208,
                        "capped": False,
                        "nindexes": 3,
                        "totalIndexSize": 557056,
                        "indexSizes": {
                            "_id_": 167936,
                            "primaryDomainId_1": 278528,
                            "_cls_1": 110592,
                        },
                        "ok": 1.0,
                    }
                }
            }
        },
        404: {
            "content": {
                "application/json": {
                    "example": {"detail": "Collection 'tissue' is not in the database"}
                }
            }
        },
    },
    summary="Collection details",
)
@cached(cache=LRUCache(maxsize=32))
def collection_details(t: str):
    """
    Returns a hash map of the details for the collection, `t`, including size (in bytes) and number of items. A collection a MongoDB concept that is analagous to a table in a RDBMS.
    An example route for this is [/protein/details](https://api.nedrex.net/protein/details).
    """
    if t not in NODE_COLLECTIONS + EDGE_COLLECTIONS:
        raise HTTPException(
            status_code=404, detail=f"Collection {t!r} is not in the database"
        )

    result = DB.command("collstats", t)
    return {k: v for k, v in result.items() if k not in ["wiredTiger", "indexDetails"]}


@router.get(
    "/{t}/all",
    responses={
        200: {"content": {"application/json": {}}},
        404: {
            "content": {
                "application/json": {
                    "example": {"detail": "Collection 'tissue' is not in the database"}
                }
            }
        },
    },
    summary="List all collection items",
)
@cached(cache=LRUCache(maxsize=32))
def list_all_collection_items(t: str):
    """
    Returns an array of all items in the collection `t`.
    Items are returned as JSON, and have all of their attributes (and corresponding values).
    An example route for this is [/drug/all](https://api.nedrex.net/drug/all), which returns all of the drugs in the database.
    Note that this route may take a while to respond, depending on the size of the collection.
    """
    if t not in NODE_COLLECTIONS + EDGE_COLLECTIONS:
        raise HTTPException(
            status_code=404, detail=f"Collection {t!r} is not in the database"
        )

    return [{k: v for k, v in i.items() if not k == "_id"} for i in DB[t].find()]


# Helper function for ID mapper
def get_primary_id(supplied_id, coll):
    result = list(DB[coll].find({"domainIds": supplied_id}))
    if result:
        return [i["primaryDomainId"] for i in result]


@router.get("/get_by_id/{t}", summary="Get by ID")
def get_by_id(t: str, q: List[str] = Query(None)):
    """
    Returns an array of items with one or more of the specified query IDs, `q`, from a collection, `t`.
    The query IDs are of the form `{database}.{accession}`, for example `uniprot.Q9UBT6`.
    An example route for this would be [/get_by_id/disorder?q=mondo.0020066&q=ncit.C92622](https://api.nedrex.net/get_by_id/disorder?q=mondo.0020066&q=ncit.C92622).
    Note that the query IDs can be a combination of (1) primary domain ID and (2) any other domain ID used to refer to an entity (e.g., `mondo.0020066` and `ncit.C92622` in the above example).
    """
    if not q:
        return []

    if t not in NODE_COLLECTIONS:
        raise HTTPException(
            status_code=404, detail=f"Collection {t!r} is not in the database"
        )

    result = DB[t].find({"domainIds": {"$in": q}})
    result = [{k: v for k, v in i.items() if not k == "_id"} for i in result]
    return result


@router.get(
    "/id_map/{t}",
    responses={
        200: {
            "content": {
                "application/json": {
                }
            }
        },
        404: {
            "content": {
                "application/json": {
                    "example": {"detail": "Collection 'tissue' is not in the database"}
                }
            }
        },
    },
    summary="ID map",
)
def id_map(t: str, q: List[str] = Query(None)):
    """
    Returns a hash map of `{user-supplied-id: [primaryDomainId]}` for a set of user-specified identifiers in a user-specified collection, `t`.
    The values in the hash map are an array because, rarely, integrated databases (e.g., MONDO) map a single external identifier onto two nodes.
    An array is returned so that the choice of how to handle this is in control of the client.
    An example route for this is [/disorder?q=mesh.C538322](https://api.nedrex.net/id_map/disorder?q=mesh.C538322),
    which returns the primary domain ID(s) for the disorder(s) cross referenced to the MeSH term C53822.
    """
    # If the user supplied no query parameters.
    if not q:
        return {}

    if t not in NODE_COLLECTIONS:
        raise HTTPException(
            status_code=404, detail=f"Collection {t!r} is not in the database"
        )
    result = {item: get_primary_id(item, t) for item in q}
    return result
