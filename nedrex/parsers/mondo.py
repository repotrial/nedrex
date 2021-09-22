#!/usr/bin/env python
from collections import defaultdict
import json
import os
from pprint import PrettyPrinter
import urllib.request
import uuid

from repodb.common import logger
from repodb.classes.nodes.disorder import Disorder
from repodb.classes.edges.disorder_is_subtype_of_disorder import DisorderIsSubtypeOfDisorder

_EXACT_MATCH = "http://www.w3.org/2004/02/skos/core#exactMatch"
_MONDO_ROOT = "http://purl.obolibrary.org/obo/MONDO_"


class MondoParser:
    def __init__(self, url="http://purl.obolibrary.org/obo/mondo.json"):
        self.url = url

    def parse_node(self, node):
        if not node["id"].startswith(_MONDO_ROOT):
            return None

        d = Disorder()
        node_id = node["id"].replace(_MONDO_ROOT, "mondo.")

        d.primaryDomainId = node_id
        d.domainIds.append(node_id)

        meta = node.get("meta")

        if meta:
            if meta.get("definition"):
                d.description = meta["definition"]["val"]

            if meta.get("basicPropertyValues"):
                exact_matches = [
                    i["val"]
                    for i in meta["basicPropertyValues"]
                    if i["pred"] == _EXACT_MATCH
                ]
                d.domainIds += self.format_uids(exact_matches)

            if meta.get("synonyms"):
                d.synonyms = [
                    i["val"] for i in meta["synonyms"] if i["pred"] == "hasExactSynonym"
                ]

            if meta.get("xrefs"):
                d.icd10 = [
                    i["val"].replace("ICD10:", "")
                    for i in meta["xrefs"]
                    if i["val"].startswith("ICD10:")
                ]

        if node.get("lbl"):
            d.displayName = node["lbl"]

        return d

    def parse_edge(self, edge):
        sub = edge["sub"]
        obj = edge["obj"]
        pred = edge["pred"]
        if not (
            sub.startswith(_MONDO_ROOT)
            and obj.startswith(_MONDO_ROOT)
            and pred == "is_a"
        ):
            return None
        diad = DisorderIsSubtypeOfDisorder(
            sourceDomainId=sub.replace(_MONDO_ROOT, "mondo."),
            targetDomainId=obj.replace(_MONDO_ROOT, "mondo."),
        )
        return diad

    def parse(self):
        logger.info("Parsing MONDO diseases")
        logger.info("\tObtaining MONDO JSON")
        with urllib.request.urlopen(self.url) as f:
            data = json.loads(f.read().decode())

        mondo = [
            i
            for i in data["graphs"]
            if i["id"] == "http://purl.obolibrary.org/obo/mondo.owl"
        ][0]
        mondo_nodes = mondo["nodes"]
        mondo_edges = mondo["edges"]

        mondo_concepts = []
        is_a_relations = []

        # Extract nodes from MONDO.
        logger.info("\tParsing nodes (disorders) from MONDO")
        disorders = []
        counter = 0

        for node in mondo_nodes:
            disorder = self.parse_node(node)
            if not disorder:
                continue

            disorders.append(disorder)
            if len(disorders) == 10_000:
                Disorder.objects.insert(disorders)
                disorders = []
                counter += 10_000
                logger.info(f"\t\tAdded 10,000 disorders ({counter:,} total)")

        if disorders:
            Disorder.objects.insert(disorders)
            counter += len(disorders)
            logger.info(f"\t\tAdded {len(disorders):,} disorders ({counter:,} total)")
            disorders = []

        # Exctact edges (is_a) from MONDO.
        logger.info("\tParsing edges (is-a relations) from MONDO")
        diads = []
        counter = 0

        for edge in mondo_edges:
            diad = self.parse_edge(edge)
            if not diad:
                continue

            diads.append(diad)
            if len(diads) == 10_000:
                DisorderIsSubtypeOfDisorder.objects.insert(diads)
                diads = []
                counter += 10_000
                logger.info(f"\t\tAdded 10,000 is-a relations ({counter:,} total)")

        if diads:
            DisorderIsSubtypeOfDisorder.objects.insert(diads)
            counter += len(diads)
            logger.info(f"\t\tAdded {len(diads):,} is-a relations ({counter:,} total)")
            diads = []

    def format_uids(self, synonym_list):
        formatted_list = []
        tups = [
            ("http://purl.obolibrary.org/obo/DOID_", "doid."),
            ("http://purl.obolibrary.org/obo/NCIT_", "ncit."),
            ("http://identifiers.org/meddra/", "meddra."),
            ("http://identifiers.org/snomedct/", "snomedct."),
            ("http://identifiers.org/omim/", "omim."),
            ("http://identifiers.org/mesh/", "mesh."),
            ("http://identifiers.org/medgen/", "medgen."),
            ("http://linkedlifedata.com/resource/umls/id/", "umls."),
            ("http://www.orpha.net/ORDO/Orphanet_", "orpha."),
        ]

        for synonym in synonym_list:
            if not any(synonym.startswith(root) for root, _ in tups):
                logger.warning(f"Synonym {synonym} does not have a matching root...")

            for root, replacement in tups:
                if synonym.startswith(root):
                    formatted_list.append(synonym.replace(root, replacement))

        return formatted_list
