# OMIM notes

The OMIM dataset comprises four datafiles:

- mim2gene.txt
- mimTitles.txt
- genemap2.txt
- morbidmap.txt

Of these, Repotrial DB currently parses `genemap2.txt`, which has ~17K entries (rows). 
`genemap2.txt` is a TSV that contains "OMIM's synopsis of the Human Gene Map, including additional information such as genomic coordinates and inheritance".
My interpretation of this is that it's effectively a dump of OMIM's knowledgebase regarding genes (e.g., chromosome location, products). 
A proportion of these also have phenotype information, and a proportion of these phenotypes are disorders.
In practice, ~8K entries that contain information on gene-disorder relationships, similar to the content of `morbidmap.txt`.

## Where is the information locates?
In the `genemap2.txt` file, there is an entrez gene ID field, and disorders are either in the phenotype field, or the MIM field (in the case of a combined gene-disorder entry).

In the `morbidmap.txt` file, the MIM number field typically maps onto a gene (entrez gene ID map can be found in the `mim2gene.txt` file). 
Similar to `genemap2.txt`, the disorder is either in the phenotype field, or the shares the same MIM number as the gene (in the case of a combined gene-disorder entry).


