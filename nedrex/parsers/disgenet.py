from collections import defaultdict
import csv
import gzip
import itertools
from pathlib import Path

import tqdm

from repodb.common import logger
from repodb.classes.nodes.disorder import Disorder
from repodb.classes.nodes.gene import Gene
from repodb.classes.edges.gene_associated_with_disorder import (
    GeneAssociatedWithDisorder,
)

class DisGeNetParser():
    def __init__(self, infile):
        self.infile = Path(infile)
        if not self.infile.exists():
            raise Exception(f'{self.infile} does not exist')

    def parse(self):
        # Create a map of UMLS identifiers to MONDO IDs
        umls_mondo = defaultdict(list)
        for item in Disorder.objects():
            for umls_id in [i for i in item.domainIds if i.startswith("umls.")]:
                umls_mondo[umls_id].append(item.primaryDomainId)

        # Create a set of valid gene IDs
        gene_ids = {i.primaryDomainId for i in Gene.objects()}

        with gzip.open(f"{self.infile}", "rt") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in tqdm.tqdm(reader):
                gene_id = f"entrez.{row['geneId'].strip()}"
                disorder_id = f"umls.{row['diseaseId'].strip()}"

                # Get the gene -- should only be one, returned as an array.
                genes = [gene_id] if gene_id in gene_ids else []
                # Get the disorder -- probably one, could be multiple depending on UMLS mapping.
                disorders = umls_mondo.get(disorder_id, [])

                # For each pair.
                for g, d in itertools.product(genes, disorders):
                    # Query to see if a relationship is already recorded.
                    gawd = GeneAssociatedWithDisorder.objects(
                        sourceDomainId = g,
                        targetDomainId = d
                    )

                    if not gawd:
                        # Create it.
                        gawd = GeneAssociatedWithDisorder(
                            sourceDomainId = g, targetDomainId = d,
                            assertedBy=["disgenet"],
                            score=float(row["score"])
                        )
                        gawd.save()
                    else:
                        # Check that there is only one result.
                        assert len(gawd) == 1
                        gawd = gawd[0]

                        if gawd.score == None:
                            new_score = float(row["score"])
                        else:
                            # This indicates a collision -- select the highest scores
                            new_score = max([float(row["score"]), gawd.score])

                        # Update
                        gawd.modify(
                            add_to_set__assertedBy = "disgenet",
                            set__score = new_score
                            )
