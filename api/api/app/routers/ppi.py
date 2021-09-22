from typing import Any, Dict, List

from fastapi import APIRouter, Query

from app.common import DB

router = APIRouter()


@router.get("/ppi", summary="Get filtered PPIs")
def get_filtered_protein_protein_interactions(iid_evidence: List[str] = Query(None),):
    """
    Returns an array of protein protein interactions (PPIs), filtered according to the evidence types given in the `?iid_evidence` query parameter(s).
    A PPI is a JSON object with "memberOne" and "memberTwo" attributes containing the primary domain IDs of the interacting proteins.
    Additional information, such as source databases and experimental methods are contained with each entry.
    The options available for `iid_evidence` are `["pred", "ortho", "exp"]`, reflecting predicted PPIs, orthologous PPIs, and experimentally detected PPIs respectively.

    An exampe route is [/ppi?iid_evidence=exp&iid_evidence=pred](https://api.nedrex.net/ppi?iid_evidence=exp&iid_evidence=pred).
    Note that there are many PPIs in the database, and so this route can take a while to respond.
    """
    query: Dict[str, Any] = {}
    if iid_evidence:
        query["evidenceTypes"] = {"$in": iid_evidence}

    results = [
        {k: v for k, v in doc.items() if not k == "_id"}
        for doc in DB["protein_interacts_with_protein"].find(query)
    ]
    return results
