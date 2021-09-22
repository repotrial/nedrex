#!/usr/bin/env python

from collections import defaultdict
import os
import itertools

from rdkit import DataStructs
import rdkit.Chem as Chem
from rdkit.Chem import AllChem, MACCSkeys

from repodb.common import logger
from repodb.classes.nodes.drug import SmallMoleculeDrug
from repodb.classes.edges.molecule_similarity_molecule import MoleculeSimilarityMolecule


def main():
    logger.info("Finding compounds with parsable SMILES")
    dct = {i.primaryDomainId: i.smiles for i in SmallMoleculeDrug.objects}
    dct = {k: v for k, v in dct.items() if v}
    dct = {k: Chem.MolFromSmiles(v) for k, v in dct.items() if Chem.MolFromSmiles(v)}

    # DEBUG
    # dct = {k:v for k,v in dct.items() if k in list(dct.keys())[:10]}

    logger.info("Creating a dictionary to hold all comparisons")
    comparisons = defaultdict(dict)
    for a, b in itertools.combinations(dct.keys(), 2):
        a, b = sorted([a, b])
        comparisons[(a, b)] = {}

    for radius in range(1, 5):
        logger.info(f"Generating Morgan Fingerprints at R{radius}")
        morgan_fps = {
            k: AllChem.GetMorganFingerprintAsBitVect(v, radius, nBits=16_384)
            for k, v in dct.items()
        }

        logger.info("Calculating scores for all pairwise comparisons")
        for a, b in comparisons.keys():
            a_fp = morgan_fps[a]
            b_fp = morgan_fps[b]

            comparisons[(a, b)][f"morgan_r{radius}"] = DataStructs.TanimotoSimilarity(
                a_fp, b_fp
            )
        del morgan_fps

    logger.info("Generating MACCS key fingerprints for all compounds")
    maccs_fps = {k: MACCSkeys.GenMACCSKeys(v) for k, v in dct.items()}

    logger.info("Calculating scores for all pairswise comparisons")
    for a, b in comparisons.keys():
        a_fp = maccs_fps[a]
        b_fp = maccs_fps[b]

        comparisons[(a, b)]["maccs"] = DataStructs.TanimotoSimilarity(a_fp, b_fp)
    del maccs_fps

    # Thresholds derived from Jasial et al (2016)
    comparisons = {
        k: v
        for k, v in comparisons.items()
        if (v["maccs"] >= 0.8 or v["morgan_r2"] >= 0.3)
    }

    for k, v in comparisons.items():
        m1, m2 = sorted(list(k))

        msm = MoleculeSimilarityMolecule()
        msm.memberOne = m1
        msm.memberTwo = m2
        msm.morganR1 = v["morgan_r1"]
        msm.morganR2 = v["morgan_r2"]
        msm.morganR3 = v["morgan_r3"]
        msm.morganR4 = v["morgan_r4"]
        msm.maccs = v["maccs"]
        msm.save()
