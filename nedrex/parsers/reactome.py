from csv import DictReader
from pathlib import Path

from repodb.common import logger
from repodb.classes.nodes.protein import Protein
from repodb.classes.nodes.pathway import Pathway
from repodb.classes.edges.protein_in_pathway import ProteinInPathway


class ReactomeParser:
    def __init__(self, uniprot_all_pathways: str = ""):
        self.upr_all_pwy = Path(uniprot_all_pathways)
        self.columns = (
            "Source database identifier",
            "Reactome Physical Entity Stable identifier",
            "Reactome Physical Entity Name",
            "Reactome Pathway Stable identifier",
            "URL",
            "Event Name",
            "Evidence code",
            "Species",
        )
        self.delimiter = "\t"

        if not self.upr_all_pwy.exists():
            raise Exception(f"{self.upr_all_pwy} does not exist.")

    def parse_pathways(self):
        with self.upr_all_pwy.open() as f:
            logger.info("Parsing UniProt to Reactome pathway map")
            # List to hold pathways.
            counter = 0

            pathway_ids = {pwy["primaryDomainId"] for pwy in Pathway.objects()}

            for row in DictReader(f, fieldnames=self.columns, delimiter=self.delimiter):
                # Only want human pathways.
                if not row["Species"] == "Homo sapiens":
                    continue
                # Get the domain ID.
                domain_id = f"reactome.{row['Reactome Pathway Stable identifier']}"
                # If the instance already exists in the DB, don't add again.
                if domain_id in pathway_ids:
                    continue

                pathway_ids.add(domain_id)

                # Construct the pathway instance.
                p = Pathway(
                    primaryDomainId=domain_id,
                    domainIds=[domain_id],
                    displayName=row["Event Name"],
                    species="Homo sapiens",
                )

                # Append pathway to pathways.
                Pathway.objects.insert(p)
                counter += 1
                # If pathways has 10_000 elements, then insert into db.
                if counter % 1_000 == 0:
                    logger.info(f"\tInserted {counter:,} pathways into RepoDB")

            logger.info(
                f"Finished adding Reactome pathways - total of {counter:,} pathways added"
            )

    def parse_protein_pathway_links(self):
        with self.upr_all_pwy.open() as f:
            logger.info("Parsing UniProt to Reactome pathway links")
            # List to hold pathways.
            counter = 0

            protein_ids = {pro["primaryDomainId"] for pro in Protein.objects()}
            protein_pathway_links = {
                (link["sourceDomainId"], link["targetDomainId"])
                for link in ProteinInPathway.objects()
            }

            for row in DictReader(f, fieldnames=self.columns, delimiter=self.delimiter):
                # Only want human pathways.
                if not row["Species"] == "Homo sapiens":
                    continue
                # Get the domain IDs.
                source_domain_id = f"uniprot.{row['Source database identifier']}"
                target_domain_id = (
                    f"reactome.{row['Reactome Pathway Stable identifier']}"
                )

                if not source_domain_id in protein_ids:
                    p = Protein(
                        primaryDomainId=source_domain_id, domainIds=[source_domain_id]
                    )
                    p.save()
                    protein_ids.add(source_domain_id)

                tup = (source_domain_id, target_domain_id)
                if not tup in protein_pathway_links:
                    pip = ProteinInPathway(
                        sourceDomainId=source_domain_id, targetDomainId=target_domain_id
                    )
                    pip.save()
                    protein_pathway_links.add(tup)

                    counter += 1

            logger.info(
                f"Finished adding Reactome pathways - total of {counter:,} pathways added"
            )
