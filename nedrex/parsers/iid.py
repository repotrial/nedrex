import csv
import gzip
from pathlib import Path

from repodb.common import logger
from repodb.classes.nodes.protein import Protein
from repodb.classes.edges.protein_protein_interaction import ProteinInteractsWithProtein


class IIDParser:
    def __init__(self, path: str, gzipped: bool = True):
        self.path = Path(path)
        self.gzipped = gzipped

    def parse(self):
        if self.gzipped:
            f = gzip.open(f"{self.path}", "rt")
        else:
            f = self.path.open()

        # Get the IDs of proteins in the database.
        protein_ids = {i.primaryDomainId for i in Protein.objects()}

        # CSV
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            uniprot_1 = f'uniprot.{row["uniprot1"]}'
            uniprot_2 = f'uniprot.{row["uniprot2"]}'

            if not uniprot_1 in protein_ids:
                logger.warning(f"{uniprot_1} not in dataset, ignoring")
                continue

            if not uniprot_2 in protein_ids:
                logger.warning(f"{uniprot_2} not in dataset, ignoring")
                continue

            evidence_type = row["evidence type"].split(";")
            databases = row["dbs"].split(";") if row["dbs"] != "-" else []
            methods = row["methods"].split(";") if row["methods"] != "-" else []

            member1, member2 = sorted([uniprot_1, uniprot_2])

            piwp = ProteinInteractsWithProtein.objects(
                memberOne=member1, memberTwo=member2
            )
            if piwp:
                piwp.evidenceTypes = list(set(evidence_type + piwp.evidenceTypes))
                piwp.databases = list(set(databases + piwp.databases))
                piwp.methods = list(set(databases + piwp.methods))
            else:
                piwp = ProteinInteractsWithProtein(
                    memberOne = member1,
                    memberTwo = member2,
                    evidenceTypes = evidence_type,
                    methods = methods,
                    databases = databases
                )
                piwp.save()
