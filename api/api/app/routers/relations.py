from collections import defaultdict
from itertools import chain
from typing import List

from fastapi import APIRouter, Response
from pydantic import BaseModel, Field

from app.common import DB

router = APIRouter()

class DisorderSeededRequest(BaseModel):
    disorders: List[str] = Field(
        None, title="Disorders", description="Disorders to get relationships for"
    )

class GeneSeededRequest(BaseModel):
    genes: List[str] = Field(
        None, title="Genes", description="Genes to get relationships for"
    )

class ProteinSeededRequest(BaseModel):
    proteins: List[str] = Field(
        None, title="Proteins", description="Proteins to get relationships for"
    )


@router.get("/get_encoded_proteins")
def get_encoded_proteins(sr: GeneSeededRequest):
    """
    Given a set of seed genes, this route returns the proteins encoded by those genes as a hash map.
    """
    genes = [
        f"entrez.{i}" if not i.startswith("entrez") else i 
        for i in sr.genes
    ]

    coll = DB["protein_encoded_by"]
    query = {"targetDomainId": {"$in": genes}}

    results = defaultdict(list)
    for i in coll.find(query):
        gene = i["targetDomainId"].replace("entrez.", "")
        protein = i["sourceDomainId"].replace("uniprot.", "")
        results[gene].append(protein)
    
    results = dict(results)
    return results


@router.get("/get_drugs_indicated_for_disorders")
def get_drugs_indicated_for_disorder(dr: DisorderSeededRequest):
    disorders = [
        f"mondo.{i}" if not i.startswith("mondo") else i
        for i in dr.disorders
    ]

    coll = DB["drug_has_indication"]
    query = {"targetDomainId": {"$in": disorders}}

    results = defaultdict(list)
    for i in coll.find(query):
        drug = i["sourceDomainId"].replace("drugbank.", "")
        disorder = i["targetDomainId"].replace("mondo.", "")
        results[disorder].append(drug)

    results = dict(results)
    return results


@router.get("/get_drugs_targetting_proteins")
def get_drugs_targetting_proteins(sr: ProteinSeededRequest):
    """
    Given a set of seed proteins, this route returns the drugs that target those proteins as a hash map.
    """
    proteins = [
        f"uniprot.{i}" if not i.startswith("uniprot") else i
        for i in sr.proteins
    ]

    coll = DB["drug_has_target"]
    query = {"targetDomainId": {"$in": proteins}}

    results = defaultdict(list)
    for i in coll.find(query):
        drug = i["sourceDomainId"].replace("drugbank.", "")
        protein = i["targetDomainId"].replace("uniprot.", "")
        results[protein].append(drug)

    results = dict(results)
    return results

@router.get("/get_drugs_targetting_gene_products")
def get_drugs_targetting_gene_products(sr: GeneSeededRequest):
    """
    Given a set of seed genes, this route returns the drugs that target the proteins encoded by the seed genes.
    Results are returned as a hash map.
    """
    genes = [
        f"entrez.{i}" if not i.startswith("entrez") else i 
        for i in sr.genes
    ]

    sr = GeneSeededRequest(genes=genes)
    gene_products = get_encoded_proteins(sr)
    all_proteins = list(chain(*gene_products.values()))

    sr = ProteinSeededRequest(proteins=all_proteins)
    drugs_targetting_proteins = get_drugs_targetting_proteins(sr)

    results = defaultdict(list)
    for gene, encoded_proteins in gene_products.items():
        for protein in encoded_proteins:
            drugs_targetting_protein = drugs_targetting_proteins.get(protein, [])
            results[gene] += drugs_targetting_protein
    
    return results
