from collections import defaultdict
from csv import DictReader
from itertools import product
from pathlib import Path
import re

from repodb.common import logger
from repodb.classes.nodes.disorder import Disorder
from repodb.classes.nodes.gene import Gene
from repodb.classes.edges.gene_associated_with_disorder import (
    GeneAssociatedWithDisorder,
)

MIM_MAP_REGEX = re.compile(r"([0-9]{6} \([0-9]\))")


class GeneMap2Parser:
    def __init__(self, path):
        self.path = Path(path)
        if not self.path.exists():
            raise Exception(f"{self.path} does not exist.")

        # Details of the file.
        self.columns = (
            "Chromosome",
            "Genomic Position Start",
            "Genomic Position End",
            "Cyto Location",
            "Computed Cyto Location",
            "MIM Number",
            "Gene Symbols",
            "Gene Name",
            "Approved Symbol",
            "Entrez Gene ID",
            "Ensembl Gene ID",
            "Comments",
            "Phenotypes",
            "Mouse Gene Symbol/ID",
        )
        self.delimiter = "\t"
        self.comment = "#"

    def parse(self):
        logger.info("Parsing OMIM gene-disease associations")

        f = self.path.open()
        r = DictReader(
            filter(lambda row: row[0] != self.comment, f),
            delimiter="\t",
            fieldnames=self.columns,
        )

        counter = 0
        total_rows_processed = 0
        row_lacking_entrez_id = 0
        row_lacking_phenotypes = 0
        no_disorder_in_repodb = 0
        no_gene_in_repodb = 0

        logger.info("\tGetting all entrez gene IDs in database")
        entrez_geneids = {i["primaryDomainId"] for i in Gene.objects()}
        logger.info("\tGenerating a map from OMIM IDs to MONDO ID")
        omim_to_mondo = defaultdict(list)
        for doc in Disorder.objects():
            omim_ids = [i for i in doc["domainIds"] if i.startswith("omim.")]
            for omim_id in omim_ids:
                omim_to_mondo[omim_id].append(doc["primaryDomainId"])

        for i in r:
            total_rows_processed += 1

            if not i["Entrez Gene ID"]:
                logger.warning("\tCannot add row; missing entrez gene ID")
                row_lacking_entrez_id += 1
                continue

            results = MIM_MAP_REGEX.findall(i["Phenotypes"])
            if not results:
                row_lacking_phenotypes += 1
                continue

            for hit in results:
                mim_number, mapping = hit[:6], hit[8]

                # Do we have the OMIM disorder in our database?
                disorders = omim_to_mondo.get(f"omim.{mim_number}")
                if not disorders:
                    logger.warning(
                        f"\tNo disorder in RepoDB with omim.{mim_number} in domain IDs"
                    )
                    no_disorder_in_repodb += 1
                    continue

                # Do we have the Entrez gene in our database?
                gene = f'entrez.{i["Entrez Gene ID"]}'
                if not gene in entrez_geneids:
                    logger.warning(
                        f"\tNo gene in RepoDB with entrez.{i['Entrez Gene ID']} in domain IDs"
                    )
                    no_gene_in_repodb += 1
                    continue

                # Very occasionally, we get multiple diseases with the same OMIM annotation. Hence, we use itertools product to assign all
                for disorder in disorders:
                    gawd = GeneAssociatedWithDisorder.objects(
                        sourceDomainId=gene, targetDomainId=disorder
                        )

                    # Doesn't exist, add it.
                    if len(gawd) == 0:
                        gawd = GeneAssociatedWithDisorder(
                            sourceDomainId=gene, targetDomainId=disorder, assertedBy=["omim"]
                        )
                        gawd.save()

                    # Already exists, ensure we've got OMIM as an asserter.
                    else:
                        assert len(gawd) == 1
                        gawd[0].modify(add_to_set__assertedBy = "omim")

                    counter += 1
                    if counter % 10_000 == 0:
                        logger.info(f"\t\tAdded {counter:,} gene-disease associations")

        if counter % 10_000 != 0:
            logger.info(f"\t\tAdded {counter:,} gene-disease associations")

        logger.debug(f"\tFinished parsing {total_rows_processed} gene-disease associations")
        logger.debug(f"\t\tCouldn't add due to OMIM record lacking Entrez gene ID: {row_lacking_entrez_id}")
        logger.debug(f"\t\tCouldn't add due to OMIM record lacking phenotypes: {row_lacking_phenotypes}")
        logger.debug(f"\t\tCouldn't add due to disorder not existing in RepoDB: {no_disorder_in_repodb}")
        logger.debug(f"\t\tCouldn't add due to gene not existing in RepoDB: {no_gene_in_repodb}")

if __name__ == "__main__":
    p = GeneMap2Parser("genemap2.txt")
    p.parse()
