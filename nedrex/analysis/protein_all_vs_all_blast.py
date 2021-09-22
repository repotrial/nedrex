#!/usr/bin/env python

from multiprocessing import cpu_count, Pool, Manager, Queue, Process, Lock
import numpy as np
import subprocess
from tempfile import NamedTemporaryFile as NTF
import threading
import time
import queue as _queue

from Bio import SearchIO, SeqIO
from more_itertools import chunked
import tqdm

from repodb.common import logger, connect, disconnect
from repodb.classes.nodes.protein import Protein
from repodb.classes.edges.protein_similarity_protein import (
    BlastHit,
    ProteinSimilarityProtein,
)


BLAST_DB = "/tmp/blastdb"
LOCK = Lock()


def run_blast(tups):
    global BLAST_DB

    seqs, evalue, seqlens, matches = tups

    tmp_fasta = NTF(suffix=".fasta", mode="w+")
    tmp_tab = NTF(suffix=".tab", mode="w+")

    SeqIO.write(seqs, tmp_fasta, "fasta")

    command = [
        "blastp",
        "-query",
        tmp_fasta.name,
        "-db",
        BLAST_DB,
        "-out",
        tmp_tab.name,
        "-outfmt",
        "6",
        "-num_threads",
        "4",
        "-evalue",
        str(evalue),
    ]
    subprocess.call(command)

    tmp_fasta.close()

    for query in SearchIO.parse(tmp_tab.name, "blast-tab"):
        for hit in query:
            if query.id == hit.id:
                continue

            best_hsp = max(hit.hsps, key=lambda i: i.bitscore)
            a, b = sorted([query.id, hit.id])
            key = (a, b)

            if not matches.get(key):
                matches[key] = []

            matches[key] = matches[key] + [
                BlastHit(
                    query=query.id,
                    hit=hit.id,
                    bitscore=best_hsp.bitscore,
                    evalue=best_hsp.evalue,
                    queryStart=int(best_hsp.query_start),
                    queryEnd=int(best_hsp.query_end),
                    hitStart=int(best_hsp.hit_start),
                    hitEnd=int(best_hsp.hit_end),
                    identity=best_hsp.ident_pct,
                    mismatches=best_hsp.mismatch_num,
                    gaps=best_hsp.gapopen_num,
                    queryCover=(best_hsp.query_end - best_hsp.query_start)
                    / seqlens[query.id],
                    hitCover=(best_hsp.hit_end - best_hsp.hit_start) / seqlens[hit.id],
                )
            ]

    tmp_tab.close()


def reader_process(queue):
    # connect(port=27020)
    pbar = tqdm.tqdm()

    to_insert = []
    while True:
        msg = queue.get()
        if msg == "DONE":
            ProteinSimilarityProtein.objects.insert(to_insert)
            pbar.update( len(to_insert) )
            to_insert = []
            break

        if not msg:
            continue

        psp = ProteinSimilarityProtein()
        psp.memberOne, psp.memberTwo = sorted([i.query for i in msg])
        psp.blast12 = [i for i in msg if i.query == psp.memberOne][0]
        psp.blast21 = [i for i in msg if i.query == psp.memberTwo][0]
        to_insert.append(psp)
        if len(to_insert) == 10_000:
            ProteinSimilarityProtein.objects.insert(to_insert)
            to_insert = []
            pbar.update(10_000)

    pbar.close()
    disconnect()
    logger.debug("Shutting down process writing entries to RepoDB")


def writer_process(dict, in_queue, out_queue):
    while True:
        try:
            in_queue.get(timeout=1)
            break
        except _queue.Empty:
            pass

        for k in (i for i in dict.keys()):
            if len(dict[k]) == 2:
                out_queue.put(dict[k])
                del dict[k]

    for k in (i for i in dict.keys()):
        if len(dict[k]) == 2:
            out_queue.put(dict[k])
            del dict[k]

    out_queue.put("DONE")
    logger.debug("Shutting down process trawling for new entries")

@logger.catch
def main(cores=16, evalue=1e-3):
    global BLAST_DB

    seq_lengths = {}
    chunk_size = 100

    logger.info(
        f"Carrying out blast all vs all for reciprocal hits ({evalue} threshold)"
    )

    logger.info("\tExtracting all sequences from the current database")
    all_records = NTF(suffix=".fasta", mode="w+")

    for doc in Protein.objects():
        if not doc.sequence:
            continue
        seq_lengths[doc.primaryDomainId] = len(doc.sequence)
        all_records.write(f">{doc.primaryDomainId}\n{doc.sequence}\n")

    # Disconnect MongoDB here -- forks should spawn their own client.
    disconnect()

    seq_lengths = {i.id: len(i.seq) for i in SeqIO.parse(all_records.name, "fasta")}

    logger.info("\tCreating blast DB")
    command = [
        "makeblastdb",
        "-in",
        all_records.name,
        "-dbtype",
        "prot",
        "-parse_seqids",
        "-hash_index",
        "-out",
        BLAST_DB,
    ]
    p = subprocess.Popen(command, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    logger.info(f"\tCarrying out blastp in chunks")
    tmp = chunked(SeqIO.parse(all_records.name, "fasta"), chunk_size)

    mgr = Manager()
    matches = mgr.dict()
    queue_1 = mgr.Queue()
    queue_2 = mgr.Queue()

    c = 0  # Iteration counter.

    reader_p = Process(target=reader_process, args=(queue_2,))
    reader_p.daemon = True
    reader_p.start()

    writer_p = Process(target=writer_process, args=(matches, queue_1, queue_2,))
    writer_p.daemon = True
    writer_p.start()

    jobs = ((seqs, evalue, seq_lengths, matches) for seqs in tmp)

    n = int(np.ceil(len(seq_lengths) / chunk_size))
    with Pool(cpu_count() // 4) as P:
        for _ in P.imap(run_blast, jobs, chunksize=1):
            # Log current progress
            c += 1
            logger.debug(f"\t\tFinished chunk {c:,} / {n:,}")

    logger.info("\tFinished running BLAST, closing FASTA file")

    queue_1.put("DONE")
    writer_p.join()
    reader_p.join()

