# Introduction.
## Components.
### Core.
The most versatile and feature-rich programs for interacting with DB.
| Program | Primary functionality |
| ------- | --------------------- |
| annotate | gets the characteristics of a tabular column from all collections of DB; the coordinate intersection is supported |
| concatenate | merges collections; there is an optional feature to remove duplicate documents |
| count | calculates quantity and frequency of elements of each set of related values of different fields |
| create | creates new or completes old DB from VCF, BED or non-standard tables; performs indexing of collections |
| dock | gets the characteristics of a tabular column from all collections and themselves tables; coordinate intersection is supported |
| ljoin | filters some collections by presence/absence of values in other collections; coordinate intersection is possible |
| info | prints names or basic properties of existing DBs |
| query | performs sets of arbitrary queries for all collections |
| reindex | removes and builds collection indexes |
| split | splits collections by values of some field |

### Plugins.
Narrowly specialized DB parsers with minimum settings.
| Program | Primary functionality |
| ------- | --------------------- |
| revitalize_id_column | fills ID column of VCF files with variant identifiers from DB |

### Scripts.
Useful tools for working with tables (not DBs).
| Program | Primary functionality |
| ------- | --------------------- |
| gen_test_files | creates N smaller tables from one table |

# Examples.
Upload the VCF set to DB. Cut the genotype data. Index nested fields with population frequencies.
```
python3 create.py -S $HOME/Bio/1000G -r -i INFO.0.AF_AFR,INFO.0.AF_EUR,INFO.0.AF_EAS
```

Annotate `name` column of multiple rsID-containing BED4 by dbSNP-VCF-based DB.
```
python3 annotate.py -S $HOME/Bio/UCSC -D dbSNP -T $HOME/Bio/out
```

Split the dbSNP-VCF-based DB by chromosomes, directing the results to new DB. Keep only fields from `#CHROM` to `ALT`. Index the `ID` field.
```
python3 split.py -D dbSNP -T dbSNP_min -k '#CHROM,POS,ID,REF,ALT' -i ID
```
