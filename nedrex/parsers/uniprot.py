import csv
import gzip
import itertools
from pathlib import Path
import re
import sys
from typing import Optional

from Bio import SeqIO
from typing_extensions import Final

from repodb.common import logger
from repodb.classes.nodes.gene import Gene
from repodb.classes.nodes.protein import Protein
from repodb.classes.edges.protein_is_isoform_of_protein import IsIsoformOf
from repodb.classes.edges.protein_encoded_by_gene import ProteinEncodedBy

csv.field_size_limit(sys.maxsize)

CURLY_REGEX = re.compile(r"{|}")
FASTA_DESCRIPTION_REGEX = re.compile(r"( [A-Z]{1,}=)")

DESCRIPTION_CUTOFF_STRINGS = ["Contains:", "Includes:", "Flags:"]
CATEGORY_FIELDS: Final = ["RecName:", "AltName:", "SubName:", ""]

SUBCATEGORY_FIELDS: Final = [
    "Full=",
    "Short=",
    "EC=",
    "Allergen=",
    "Biotech=",
    "CD_antigen=",
    "INN=",
]

tmp = [" ".join(i) for i in itertools.product(CATEGORY_FIELDS, SUBCATEGORY_FIELDS)]
tmp.sort(key=lambda i: len(i), reverse=True)
COMBINATION_FIELDS: Final = re.compile(r"|".join(tmp))
del tmp

idmap_headings = (
    "UniProtKB-AC",
    "UniProtKB-ID",
    "GeneID (EntrezGene)",
    "RefSeq",
    "GI",
    "PDB",
    "GO",
    "UniRef100",
    "UniRef90",
    "UniRef50",
    "UniParc",
    "PIR",
    "NCBI-taxon",
    "MIM",
    "UniGene",
    "PubMed",
    "EMBL",
    "EMBL-CDS",
    "Ensembl",
    "Ensembl_TRS",
    "Ensembl_PRO",
    "Additional PubMed",
)


def process_record(record):
    """
    Get the information from the record that we'd want to store in the database.
    """
    p = Protein()
    # Domain IDs
    p.primaryDomainId = f"uniprot.{record.id}"
    p.domainIds = [f"uniprot.{record.id}"]
    # Sequence
    p.sequence = str(record.seq)
    # Display name
    p.displayName = record.name

    # Assertion statement checking that TaxID has a length of 1.
    taxid = record.annotations["ncbi_taxid"]
    p.taxid = int(taxid[0])

    # Find the first appearance of a cutoff string.
    synonyms = record.description.split()
    cutoff = next(
        (
            val
            for val, item in enumerate(synonyms)
            if item in DESCRIPTION_CUTOFF_STRINGS
        ),
        999_999,
    )
    synonyms = " ".join(synonyms[:cutoff])
    synonyms = COMBINATION_FIELDS.split(synonyms)
    # Strip items
    synonyms = [i.strip() for i in synonyms]
    # Remove trailing ;
    synonyms = [i[:-1] if i.endswith(";") else i for i in synonyms]
    # Remove empty strings.
    p.synonyms = [i for i in synonyms if i]

    # Find the gene name (if given).
    gene_name = record.annotations.get("gene_name", "")
    if not gene_name:
        pass
    else:
        if gene_name.startswith("Name="):
            gene_name = gene_name.replace("Name=", "").split(";", 1)[0]
            p.geneName = CURLY_REGEX.split(gene_name)[0].strip()

    # Extract the comments; leave as free-text.
    comments = record.annotations.get("comment", "")
    if not comments:
        pass
    else:
        p.comments = comments

    return p


class UniprotParser:
    def __init__(
        self,
        *,
        swiss: str = "",
        trembl: str = "",
        splicevar: str = "",
        idmap: str = "",
        gzipped: bool = True,
    ):
        self.swiss = Path(swiss)
        self.trembl = Path(trembl)
        self.splicevar = Path(splicevar)
        self.idmap = Path(idmap)
        self.gzipped = gzipped

        if not self.swiss.exists():
            raise Exception(f"{self.swiss} does not exist")
        if not self.trembl.exists():
            raise Exception(f"{self.trembl} does not exist")
        if not self.splicevar.exists():
            raise Exception(f"{self.splicevar} does not exist")

    def parse_splice_variants(self, file):
        if self.gzipped:
            f = gzip.open(f"{file}", "rt")
        else:
            f = open(f"{file}", "r")

        logger.info("\tParsing splice variants")
        logger.info(
            "\t\tExtracting IDs of existing protein objects to check parents exist in db"
        )
        protein_ids = {i["primaryDomainId"] for i in Protein.objects()}
        logger.info("\t\tFinished extracting existing protein objects")

        counter = 0
        for record in SeqIO.parse(f, "fasta"):
            # Tease apart the description to get the properties.
            properties = {}
            desc = record.description

            x = FASTA_DESCRIPTION_REGEX.findall(desc)

            for k in x[::-1]:
                desc, val = desc.split(k)
                properties[k[1:-1]] = val
            desc = desc.replace(record.id, "").strip()
            properties["description"] = desc
            (_, properties["id"], properties["name"]) = record.id.split("|")
            properties["id"] = f"uniprot.{properties['id']}"

            if not properties["OX"] == "9606":
                continue

            p = Protein(
                primaryDomainId=properties["id"],
                domainIds=[properties["id"]],
                sequence=str(record.seq),
                displayName=properties["name"],
                taxid=9606,
                synonyms=[properties["description"]],
            )
            if properties.get("GN"):
                p.geneName = properties["GN"]
            p.save()

            main_record_id = properties["id"].split("-")[0]

            if not main_record_id in protein_ids:
                logger.warning(
                    f"\t\tExpected {main_record_id} (for {properties['id']}) to be in database, but not present - skipping"
                )
                continue

            iio = IsIsoformOf(
                sourceDomainId=properties["id"], targetDomainId=main_record_id
            )
            iio.save()
            counter += 1
            if counter % 10_000 == 0:
                logger.info(f"\t\tAdded {counter:,} isoforms to the database")
        if not counter % 10_000 == 0:
            logger.info(f"\t\tAdded {counter:,} isoforms to the database")
        logger.info(f"\tFinished parsing isoforms")

    def parse_file(self, file):
        logger.info(f"\tParsing {file}")
        if self.gzipped:
            f = gzip.open(f"{file}", "rt")
        else:
            f = open(f"{file}", "r")

        lst = []
        counter = 0

        for record in SeqIO.parse(f, "swiss"):
            lst.append(process_record(record))
            if len(lst) == 10_000:
                Protein.objects.insert(lst)
                lst = []
                counter += 10_000
                logger.info(f"\t\tAdded 10,000 proteins ({counter:,} total)")

        if lst:
            Protein.objects.insert(lst)
            counter += len(lst)
            logger.info(f"\t\tAdded {len(lst):,} proteins ({counter:,} total)")

            lst = []

        logger.info(f"\tFinished parsing {file}")

        f.close()

    def parse_idmap(self):
        logger.info("Parsing UniProt ID map")
        if self.gzipped:
            f = gzip.open(f"{self.idmap}", "rt")
        else:
            f = self.idmap.open()

        logger.info("Obtaining gene and protein IDs for existing entities")
        # Get all genes IDs.
        all_genes = {i.primaryDomainId for i in Gene.objects()}
        # Get all protein IDs
        all_proteins = {i.primaryDomainId for i in Protein.objects()}

        reader = csv.DictReader(f, delimiter="\t", fieldnames=idmap_headings)
        for row in reader:
            upr_acc = f'uniprot.{row["UniProtKB-AC"]}'
            entrez_genes = [
                f"entrez.{i.strip()}"
                for i in row["GeneID (EntrezGene)"].split(";")
                if i.strip()
            ]

            if not upr_acc in all_proteins:
                logger.info(f"\tProtein {upr_acc} not in database, skipping")
                continue

            for gene in entrez_genes:
                if not gene in all_genes:
                    logger.info(f"\tGene {gene} not in database, skipping")
                    continue

                peb = ProteinEncodedBy(sourceDomainId=upr_acc, targetDomainId=gene)
                peb.save()

    def parse_proteins(self):
        logger.info("Parsing UniProt data files")
        self.parse_file(self.swiss)
        self.parse_file(self.trembl)
        self.parse_splice_variants(self.splicevar)
