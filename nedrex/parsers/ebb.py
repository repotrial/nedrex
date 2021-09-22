from collections import defaultdict
import csv
from itertools import product
from pathlib import Path

from repodb.common import logger
from repodb.classes.nodes.disorder import Disorder
from repodb.classes.edges.disorder_comorbid_with_disorder import (
    DisorderComorbidWithDisorder,
)


class EBBParser:
    def __init__(self, phicor, rr):
        self.phicor = Path(phicor)
        self.rr = Path(rr)

        if not self.phicor.exists():
            raise Exception(f"{self.phicor} does not exist")

        if not self.rr.exists():
            raise Exception(f"{self.rr} does not exist")

    def parse(self):
        # Make a map from ICD-10 codes to MONDO identifiers.
        icd10_mondo_map = defaultdict(list)
        for d in Disorder.objects():
            for icd10 in d.icd10:
                # Only interested in 3 char ICD codes.
                if not len(icd10) == 3:
                    continue
                icd10_mondo_map[icd10].append(d.primaryDomainId)

        logger.info("Parsing comorbiditome")
        logger.info("\tParsing PhiCor file")
        with self.phicor.open() as f:
            counter = 0
            mappable = 0
            for row in csv.DictReader(f, delimiter="\t"):
                counter += 1
                d1 = row["disease1"]
                d2 = row["disease2"]

                # Do we have these disorders?
                if not icd10_mondo_map.get(d1):
                    # logger.warning(f"\tNo disorders with ICD-10 code {d1}; dropping")
                    continue
                if not icd10_mondo_map.get(d2):
                    # logger.warning(f"\tNo disorders with ICD-10 code {d2}; dropping")
                    continue

                mappable += 1
                if mappable % 1_000 == 0:
                    logger.debug(
                        f"\t\tPhiCor {mappable:,}/{counter:,} ({mappable / counter * 100:.2f}%)"
                    )

                for mondo1, mondo2 in product(icd10_mondo_map[d1], icd10_mondo_map[d2]):
                    phicor = float(row["phi_cor"])
                    matches = DisorderComorbidWithDisorder.objects(
                        memberOne=mondo1, memberTwo=mondo2
                    )
                    if matches:
                        matches.update_one(set__phiCor=phicor, upsert=True)
                    else:
                        dcwd = DisorderComorbidWithDisorder(
                            memberOne=mondo1,
                            memberTwo=mondo2,
                            phiCor=phicor
                        )
                        dcwd.save()

            logger.debug(
                f"\tFinal PhiCor {mappable:,}/{counter:,} ({mappable / counter * 100:.2f}%)"
            )

        with self.rr.open() as f:
            counter = 0
            mappable = 0
            for row in csv.DictReader(f, delimiter="\t"):
                counter += 1
                d1 = row["disease1"]
                d2 = row["disease2"]

                # Do we have these disorders?
                if not icd10_mondo_map.get(d1):
                    # logger.warning(f"\tNo disorders with ICD-10 code {d1}; dropping")
                    continue
                if not icd10_mondo_map.get(d2):
                    # logger.warning(f"\tNo disorders with ICD-10 code {d2}; dropping")
                    continue

                mappable += 1
                if mappable % 1_000 == 0:
                    logger.debug(
                        f"\t\tRR {mappable:,}/{counter:,} ({mappable / counter * 100:.2f}%)"
                    )

                for mondo1, mondo2 in product(icd10_mondo_map[d1], icd10_mondo_map[d2]):
                    rr12 = row["RR12"]
                    rr21 = row["RR21"]
                    rr_geo_mean = row["GeoMeanRR"]

                    matches = DisorderComorbidWithDisorder.objects(
                        memberOne=mondo1, memberTwo=mondo2
                    )

                    if matches:
                        matches.update_one(
                            set__rr12=rr12,
                            set__rr21=rr21,
                            set__rrGeoMean=rr_geo_mean,
                            upsert=True,
                        )
                    else:
                        dcwd = DisorderComorbidWithDisorder(
                            memberOne=mondo1, memberTwo=mondo2,
                            rr12 = rr12, rr21 = rr21, rrGeoMean=rr_geo_mean
                        )
                        dcwd.save()

            logger.debug(
                f"\tFinal RR {mappable:,}/{counter:,} ({mappable / counter * 100:.2f}%)"
            )
