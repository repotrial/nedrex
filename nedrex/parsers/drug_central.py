#!/usr/bin/env python

"""
This file integrates Drug Central drug-disease indication edges into RepoDB.

Drug Central cross-references do not seem to cross reference to DrugBank identifiers,
and so integration is performed using CAS Registration Number for drugs and
SNOMED-CT ID for disorders.
"""

import csv
from itertools import product
from pathlib import Path

from repodb.common import logger
from repodb.classes.nodes.drug import Drug
from repodb.classes.nodes.disorder import Disorder
from repodb.classes.nodes.protein import Protein

from repodb.classes.edges.drug_has_target import DrugHasTarget
from repodb.classes.edges.drug_has_indication import DrugHasIndication
from repodb.classes.edges.drug_has_contraindication import DrugHasContraindication


class DrugCentralParser:
    def __init__(self, indications, identifier, targets):
        self.indications = Path(indications)
        if not self.indications.exists():
            raise Exception(f"{self.indications} does not exist")

        self.identifier = Path(identifier)
        if not self.identifier.exists():
            raise Exception(f"{self.identifier} does not exist")

        self.targets = Path(targets)
        if not self.targets.exists():
            raise Exception(f"{self.targets} does not exist")

    def parse(self):
        # First, get the drug central synonyms (maps from drug central identifiers to DrugBank identifiers)
        dc_syn = {}
        with self.identifier.open() as f:
            reader = csv.DictReader(f)
            for item in reader:
                if not item["id_type"] == "DRUGBANK_ID":
                    continue
                dc_syn[int(item["struct_id"])] = item

        with self.indications.open() as f:
            reader = csv.DictReader(f)
            for item in reader:
                # Filter non-indication relationships
                if not item["relationship_name"] in ("indication", "contraindication"):
                    continue
                # Attempt to get cross references
                drugbank_details = dc_syn.get(int(item["struct_id"]))
                if not drugbank_details:
                    logger.warning(
                        f"Drug {item['struct_id']} does not have a Drugbank cross reference to map with, dropping."
                    )
                    continue

                if not item["snomed_conceptid"]:
                    logger.warning(
                        f"Disorder does not have a SNOMEDCT cross reference to map with, dropping."
                    )
                    continue

                # Create identifiers
                _a = float(item["snomed_conceptid"])
                _a = int(_a)

                dbid = f"drugbank.{drugbank_details['identifier']}"
                scid = f"snomedct.{_a}"

                # Find matches.
                drugs = Drug.objects(domainIds=dbid)
                disorders = Disorder.objects(domainIds=scid)

                if not drugs:
                    logger.warning(
                        f"Drug with DrugBank ID ({dbid}) does not exist in repodb, dropping"
                    )
                    continue

                if not disorders:
                    logger.warning(
                        f"Disorder with SNOMED-CT ({scid}) does not exist in repodb, dropping"
                    )
                    continue

                for drug, disorder in product(drugs, disorders):
                    if item["relationship_name"] == "indication":
                        dhi = DrugHasIndication.objects(
                            sourceDomainId=drug.primaryDomainId,
                            targetDomainId=disorder.primaryDomainId
                        ).first()
                        if dhi:
                            dhi.databases.append("DrugCentral")
                            dhi.databases = list(set(dhi.databases))
                            dhi.save()
                            continue

                        dhi = DrugHasIndication(
                            sourceDomainId=drug.primaryDomainId,
                            targetDomainId=disorder.primaryDomainId,
                            databases = ["DrugCentral"]
                        )
                        dhi.save()

                    if item["relationship_name"] == "contraindication":
                        dhc = DrugHasContraindication.objects(
                            sourceDomainId=drug.primaryDomainId,
                            targetDomainId=disorder.primaryDomainId
                        ).first()
                        if dhc:
                            continue

                        dhc = DrugHasContraindication(
                            sourceDomainId=drug.primaryDomainId,
                            targetDomainId=disorder.primaryDomainId,
                        )
                        dhc.save()

        with self.targets.open() as f:
            reader = csv.DictReader(f)
            for item in reader:
                drugbank_details = dc_syn.get(int(item["struct_id"]))
                if not drugbank_details:
                    logger.warning(
                        f"Drug {item['struct_id']} does not have a Drugbank cross reference to map with, dropping."
                    )
                    continue

                uniprot_accessions = [i.strip() for i in item["accession"].split("|") if i.strip()]
                upids = [f"uniprot.{i}" for i in uniprot_accessions]
                dbid = f"drugbank.{drugbank_details['identifier']}"

                for upid in upids:
                    drugs = Drug.objects(domainIds=dbid)
                    proteins = Protein.objects(domainIds=upid)
                    if not drugs:
                        logger.warning(
                            f"Drug with DrugBank ID ({dbid}) does not exist in repodb, dropping"
                        )
                        continue

                    if not proteins:
                        logger.warning(
                            f"Protein with UniProt ID ({upid}) does not exist in repodb, dropping"
                        )
                        continue

                    for drug, protein in product(drugs, proteins):
                        dht = DrugHasTarget.objects(sourceDomainId=drug.primaryDomainId, targetDomainId=protein.primaryDomainId).first()
                        if dht:
                            dht.databases.append("DrugCentral")
                            dht.databases = list(set(dht.databases))
                            dht.save()
                        else:
                            dht = DrugHasTarget(
                                sourceDomainId=drug.primaryDomainId,
                                targetDomainId=protein.primaryDomainId,
                                databases = ["DrugCentral"]
                            )
                            dht.save()
