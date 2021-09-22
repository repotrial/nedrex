#!/usr/bin/env python
from collections import OrderedDict
from pathlib import Path
from pprint import PrettyPrinter
import uuid
import xml.etree.cElementTree as et

from Bio.Alphabet.IUPAC import (
    IUPACAmbiguousDNA,
    IUPACAmbiguousRNA,
    ExtendedIUPACProtein,
)

from xmljson import badgerfish as bf

from repodb.common import logger
from repodb.classes.nodes.drug import SmallMoleculeDrug, BiotechDrug
from repodb.classes.nodes.protein import Protein
from repodb.classes.edges.drug_has_target import DrugHasTarget


def recursive_yield(elem):
    if isinstance(elem, OrderedDict):
        yield elem
    else:
        for i in elem:
            yield from recursive_yield(i)


def determinal_alphabet(sequence):
    matches_dna = all(i in IUPACAmbiguousDNA.letters for i in sequence)
    matches_rna = all(i in IUPACAmbiguousRNA.letters for i in sequence)
    matches_protein = all(i in ExtendedIUPACProtein.letters for i in sequence)

    if matches_dna and matches_rna:
        raise Exception(f"Unable to determine biomolecule type: {sequence}")
    # If there's a match for DNA and not RNA, then assume RNA.
    if matches_dna and not matches_rna:
        return "D"
    # If there's a match for RNA and not DNA, then assume DNA.
    if matches_rna and not matches_dna:
        return "R"
    # If there's a match from the protein alphabet, assume protein.
    if matches_protein:
        return "P"
    else:
        raise Exception(f"Unable to determine biomolecule type: {sequence}")


ns = lambda i: "{http://www.drugbank.ca}" + i


class DrugBankParser:
    def __init__(self, drugbank_path):
        self.path = Path(drugbank_path)
        if not self.path.exists():
            raise Exception(f"{self.path} does not exist")

    def get_drugs(self):
        # Gets drugs at the top level.
        handle = self.path.open()

        depth = 0
        for event, elem in et.iterparse(handle, events=["start", "end"]):
            if not elem.tag == ns("drug"):
                continue

            if event == "start":
                depth += 1
            if event == "end":
                depth -= 1

            if depth == 0 and event == "end":
                yield bf.data(elem)[ns("drug")]

        handle.close()

    def get_targets(self, drug):
        targets = drug[ns("targets")]
        if not ns("target") in targets.keys():
            return

        targets = targets[ns("target")]
        yield from recursive_yield(targets)

    def get_molecule_details(self, drug):
        # Extracts small molecule details from the drug element.
        c = {"iupacName": None, "smiles": None, "inchi": None, "molecularFormula": None}

        properties = drug[ns("calculated-properties")]
        if not ns("property") in properties.keys():
            return c

        properties = properties[ns("property")]

        if isinstance(properties, OrderedDict):
            properties = [properties]

        for i in properties:
            if i[ns("kind")]["$"] == "InChI":
                c["inchi"] = i[ns("value")]["$"]
            if i[ns("kind")]["$"] == "SMILES":
                c["smiles"] = i[ns("value")]["$"]
            if i[ns("kind")]["$"] == "IUPAC Name":
                c["iupacName"] = i[ns("value")]["$"]
            if i[ns("kind")]["$"] == "Molecular Formula":
                c["molecularFormula"] = i[ns("value")]["$"]

        return c

    def get_protein_details(self, drug):
        # Extracts protein details from the drug element.
        p = {}
        seqs = drug[ns("sequences")]
        if not ns("sequence") in seqs.keys():
            return {"sequences": []}

        seqs = seqs[ns("sequence")]

        if isinstance(seqs, OrderedDict):
            seqs = [seqs]

        formats = {i["@format"] for i in seqs}
        if not formats == {"FASTA"}:
            logger.warning(
                "Expected all data to be in FASTA format -- some sequence data may be missed"
            )

        seqs = [i for i in seqs if i["@format"] == "FASTA"]
        seqs = [i["$"].split("\n", 1) for i in seqs]
        seqs = [tuple([i[0][1:], i[1]]) for i in seqs]
        seqs = [
            ">{} {}\n{}".format(uuid.uuid4(), desc, seq.replace("\n", ""))
            for desc, seq in seqs
        ]
        p["sequences"] = seqs

        return p

    @logger.catch
    def parse(self):
        # Parse the drugs first
        logger.info("Parsing DrugBank")
        logger.info("\tParsing drugs from DrugBank")

        # Get known protein IDs
        logger.info("\tGetting protein IDs for proteins in database")
        protein_ids = {pro["primaryDomainId"] for pro in Protein.objects}
        logger.info("\tProteins IDs obtained")

        bt_drugs = []
        sm_drugs = []
        counter = 0

        p = PrettyPrinter()

        for drug in self.get_drugs():
            if drug["@type"] == "biotech":
                d = BiotechDrug()
            elif drug["@type"] == "small molecule":
                d = SmallMoleculeDrug()
            else:
                raise Exception("Unexpected DrugBank type")

            d.allDatasets.append("DrugBank")
            d.primaryDataset = "DrugBank"

            # IDs
            drug_ids = drug[ns("drugbank-id")]
            if isinstance(drug_ids, OrderedDict):
                drug_ids = [drug_ids]

            for drug_id in drug_ids:
                if "@primary" in drug_id:
                    d.primaryDomainId = f'drugbank.{drug_id["$"]}'
                d.domainIds.append(f'drugbank.{drug_id["$"]}')

            # Sort targets
            for t in self.get_targets(drug):
                # Question 1 - Does the target have a known action?
                actions = t.get(ns("actions"))
                if actions:
                    actions = [i["$"] for i in recursive_yield(actions[ns("action")])]
                else:
                    actions = []

                # Question 2 - is the target a Polypeptide? If not, skip.
                polypeptide = t.get(ns("polypeptide"))
                if not polypeptide:
                    continue

                for i in recursive_yield(polypeptide):
                    if not i["@source"] in ["TrEMBL", "Swiss-Prot"]:
                        continue
                    domainId = f"uniprot.{i['@id']}"

                    # Do we have this protein in our dataset? If not, add
                    if not domainId in protein_ids:
                        p = Protein(
                            primaryDomainId=domainId, domainIds=[domainId], taxid=-1
                        )
                        p.save()
                        protein_ids.add(domainId)

                    # Create drug-has-target instance
                    dht = DrugHasTarget()
                    dht.sourceDomainId = d.primaryDomainId
                    dht.targetDomainId = domainId
                    dht.actions = actions
                    dht.databases = ["DrugBank"]
                    dht.save()

            # Name
            d.displayName = drug[ns("name")]["$"]

            # Synonyms
            synonyms = drug[ns("synonyms")]
            if not ns("synonym") in synonyms.keys():
                pass
            else:
                synonyms = synonyms[ns("synonym")]
                if isinstance(synonyms, OrderedDict):
                    synonyms = [synonyms]
                for synonym in synonyms:
                    d.synonyms.append(synonym["$"])

            # Categories
            categories = drug[ns("categories")]
            if not ns("category") in categories.keys():
                pass
            else:
                categories = categories[ns("category")]
                if isinstance(categories, OrderedDict):
                    categories = [categories]
                for category in categories:
                    d.drugCategories.append(
                        category[ns("category")].get("$")
                        # category[ns("mesh-id")].get("$")
                    )

            # Description
            description = drug[ns("description")]
            if "$" in description:
                d.description = description["$"]

            # Cas registration number
            cas_number = drug[ns("cas-number")]
            assert isinstance(cas_number, OrderedDict)
            if "$" in cas_number:
                d.casNumber = drug[ns("cas-number")]["$"]

            # Indications
            indications = drug[ns("indication")]
            if "$" in indications.keys():
                d.indication = indications["$"]

            # Groups
            if ns("groups") in drug.keys():
                groups = drug[ns("groups")][ns("group")]
                if isinstance(groups, OrderedDict):
                    groups = [groups]
                for group in groups:
                    d.drugGroups.append(group["$"])

            # Add drug type-specific properties.
            if d.type == "SmallMoleculeDrug":
                details = self.get_molecule_details(drug)
                d.iupacName = details["iupacName"]
                d.inchi = details["inchi"]
                d.smiles = details["smiles"]
                d.molecularFormula = details["molecularFormula"]

                sm_drugs.append(d)
                counter += 1

            elif d.type == "BiotechDrug":
                details = self.get_protein_details(drug)
                d.sequences = details["sequences"]

                bt_drugs.append(d)
                counter += 1

            # Check for iterative update.
            if counter % 100 == 0:
                if bt_drugs:
                    BiotechDrug.objects.insert(bt_drugs)
                    bt_drugs = []
                if sm_drugs:
                    SmallMoleculeDrug.objects.insert(sm_drugs)
                    sm_drugs = []

                logger.info(f"\t\tAdded 100 drugs ({counter:,} total)")

        if not counter % 100:
            if bt_drugs:
                BiotechDrug.objects.insert(bt_drugs)
            if sm_drugs:
                SmallMoleculeDrug.objects.insert(sm_drugs)

            logger.info(
                f"\t\tAdded {len(bt_drugs) + len(sm_drugs)} drugs ({counter:,} total)"
            )

            sm_drugs = []
            bt_drugs = []
