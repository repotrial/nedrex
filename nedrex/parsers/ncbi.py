from csv import DictReader
import gzip
from pathlib import Path

from repodb.common import logger
from repodb.classes.nodes.gene import Gene


class NCBIGeneInfoParser:
    def __init__(self, path):
        self.path = Path(path)
        if not self.path.exists():
            raise Exception(f"{self.path} does not exist")

        self.columns = (
            "tax_id",
            "GeneID",
            "Symbol",
            "LocusTag",
            "Synonyms",
            "dbXrefs",
            "chromosome",
            "map_location",
            "description",
            "type_of_gene",
            "Symbol_from_nomenclature_authority",
            "Full_name_from_nomenclature_authority",
            "Nomenclature_status",
            "Other_designations",
            "Modification_date",
            "Feature_type",
        )
        self.comment = "#"
        self.delimiter = "\t"

    def parse(self):
        logger.info("Parsing NCBI Gene Info")
        with gzip.open(f"{self.path}", "rt") as f:
            genes = []
            counter = 0
            for row in DictReader(
                filter(lambda row: row[0] != self.comment, f),
                delimiter=self.delimiter,
                fieldnames=self.columns,
            ):
                g = Gene()
                g.primaryDomainId = f'entrez.{row["GeneID"]}'
                g.domainIds.append(f"entrez.{row['GeneID']}")

                approved_symbol = row["Symbol_from_nomenclature_authority"].strip()
                if not approved_symbol == "-":
                    g.approvedSymbol = approved_symbol
                    g.displayName = approved_symbol
                else:
                    g.displayName = f'entrez.{row["GeneID"]}'

                g.symbols = row["Synonyms"].split("|")
                g.description = row["description"]

                g.synonyms = row["Other_designations"].split("|")
                full_name = row["Full_name_from_nomenclature_authority"].strip()
                if full_name != "-" and full_name not in g.synonyms:
                    g.synonyms.append(full_name)

                g.synonyms = row["Other_designations"].split("|")
                g.chromosome = row["chromosome"]
                g.mapLocation = row["map_location"]
                g.geneType = row["type_of_gene"]

                genes.append(g)
                if len(genes) == 10_000:
                    Gene.objects.insert(genes)
                    genes = []
                    counter += 10_000
                    logger.info(f"\tAdded 10,000 genes ({counter:,} total)")

            if genes:
                Gene.objects.insert(genes)
                counter += len(genes)
                logger.info(f"\tAdded {len(genes)} genes ({counter:,} total)")
                genes = []
