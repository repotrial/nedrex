import logging
from functools import lru_cache
from multiprocessing import Lock
from pathlib import Path
from uuid import uuid4

from neo4j import GraphDatabase
from pymongo import MongoClient

from app.config import get_settings

settings = get_settings()

# Set up the logger
logger = logging.getLogger("base")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("api.log")

formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
fh.setFormatter(formatter)
logger.addHandler(fh)

CLIENT = MongoClient(settings.mongodb_url)
DB = CLIENT[settings.mongodb_db]
DB_LOCK = Lock()

NEO4J_DRIVER = GraphDatabase.driver(settings.neo4j_url)

RESULTS_DIR = Path(settings.results_dir)
if not RESULTS_DIR.exists():
	raise Exception("Results directory does not exist!")
STATIC_DIR = Path(settings.static_dir)


NODE_COLLECTIONS = sorted(
    [
        "disorder",
        "drug",
        "gene",
        "pathway",
        "protein",
        "signature"
    ]
)

EDGE_COLLECTIONS = sorted(
    [
        # "disorder_comorbid_with_disorder",
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
)

@lru_cache(maxsize=None)
def get_network(query, prefix):
    outfile = f"/tmp/{uuid4()}.tsv"

    logger.info("Running query to get edges in PPI network.")
    with NEO4J_DRIVER.session() as session, open(outfile, "w") as f:
        for result in session.run(query):
            a = result["x.primaryDomainId"].replace(prefix, "")
            b = result["y.primaryDomainId"].replace(prefix, "")
            f.write("{}\t{}\n".format(a, b))

    return outfile
