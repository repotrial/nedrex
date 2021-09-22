import os

from dotenv import load_dotenv

import repodb

load_dotenv()

print(f'PORT: {os.environ["MONGO_PORT"]}')
repodb.common.connect(port=int(os.environ["MONGO_PORT"]))


rp = repodb.parsers.reactome.ReactomeParser(
    "./datasets/reactome/UniProt2Reactome_PE_All_Levels.txt"
)
rp.parse_pathways()

up = repodb.parsers.uniprot.UniprotParser(
    swiss="./datasets/uniprot/uniprot_sprot_human.dat.gz",
    trembl="./datasets/uniprot/uniprot_trembl_human.dat.gz",
    splicevar="./datasets/uniprot/uniprot_sprot_varsplic.fasta.gz",
    idmap="./datasets/uniprot/HUMAN_9606_idmapping_selected.tab.gz",
)

up.parse_proteins()

mp = repodb.parsers.mondo.MondoParser()
mp.parse()

ncbi = repodb.parsers.ncbi.NCBIGeneInfoParser("datasets/ncbi/Homo_sapiens.gene_info.gz")
ncbi.parse()

op = repodb.parsers.omim.GeneMap2Parser("datasets/omim/genemap2.txt")
op.parse()

dgnp = repodb.parsers.disgenet.DisGeNetParser(infile="datasets/disgenet/curated_gene_disease_associations.tsv.gz")
dgnp.parse()

db = repodb.parsers.drugbank.DrugBankParser("datasets/drugbank/full database.xml")
db.parse()

rp.parse_protein_pathway_links()

ip = repodb.parsers.iid.IIDParser("datasets/iid/human_annotated_PPIs.txt.gz")
ip.parse()

up.parse_idmap()

dp = repodb.parsers.drug_central.DrugCentralParser(
    identifier="./datasets/drug_central/identifier.csv",
    indications="./datasets/drug_central/omop_relationship.csv",
    targets="./datasets/drug_central/act_table_full.csv"
)
dp.parse()

ep = repodb.parsers.ebb.EBBParser(
    phicor="datasets/ebb/comorbidity_estonia_filtered_3char_phiCor_positive.txt",
    rr="datasets/ebb/comorbidity_estonia_filtered_3char_RR_cutoff1.txt",
)
ep.parse()

repodb.analysis.molecule_all_vs_all.main()
repodb.analysis.protein_all_vs_all_blast.main()
