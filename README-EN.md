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

## Preparation.
1. [Install](https://www.mongodb.com/docs/manual/installation/#mongodb-installation-tutorials) _MongoDB_. Don't forget to activate it via `sudo systemctl enable mongod.service` command.
2. Install _PyMongo_:
```
pip3 install pymongo
```
or
```
conda install pymongo
```
3. [Download](https://github.com/PlatonB/high-perf-bio/archive/refs/heads/master.zip) the archive with toolkit.
4. Unzip it into any directory.
5. Go to the `high-perf-bio-master` directory in the terminal.
```
cd /path/to/high-perf-bio-master
```
### Preparing quick GUI launch.
If the following instructions don't fit, start the GUI by `streamlit run _gui_streamlit.py` command.

#### Gnome.
Right click on `__run_gui_streamlit.sh` --> `Properties` --> `Permissions` --> `Allow executing file as program`.
After that, in the context menu will appear `Run as a Program` item.

#### KDE.
Right click on `__run_gui_streamlit.sh` --> `Properties` --> `Permissions` --> `Is executable` --> `OK`;
right click on `__run_gui_streamlit.sh` --> `Open With` --> `Other Application` --> `System` --> `Konsole` --> `OK`;
close `Konsole`.
Now it is possible to run the GUI by left-clicking on the shortcut.

# Quick start.
Upload the VCF set to DB. Cut the genotype data. Index nested fields with population frequencies.
```
python3 create.py -S $HOME/Bio/1000G -r -i INFO.0.AF_AFR,INFO.0.AF_EUR,INFO.0.AF_EAS
```

Execute 2 sets of queries from [our MCCMB-2021 work](http://mccmb.belozersky.msu.ru/2021/thesis/abstracts/402_MCCMB_2021.pdf).

$HOME/Bio/pop_freq_queries/bias_1.txt:
```
{'INFO.0.AF_AFR': {'$lt': Decimal128('0.02')}, 'INFO.0.AF_EUR': {'$gt': Decimal128('0.3')}, 'INFO.0.AF_EAS': {'$gt': Decimal128('0.3')}}
{'INFO.0.AF_AFR': {'$lt': Decimal128('0.02')}, 'INFO.0.AF_EUR': {'$gt': Decimal128('0.3')}, 'INFO.0.AF_EAS': {'$lt': Decimal128('0.02')}}
{'INFO.0.AF_AFR': {'$lt': Decimal128('0.02')}, 'INFO.0.AF_EUR': {'$lt': Decimal128('0.02')}, 'INFO.0.AF_EAS': {'$gt': Decimal128('0.3')}}
```

$HOME/Bio/pop_freq_queries/bias_2.txt:
```
{'$or': [{'INFO.0.AF_AFR': {'$gte': Decimal128('0.02')}}, {'INFO.0.AF_EUR': {'$lte': Decimal128('0.3')}}, {'INFO.0.AF_EAS': {'$lte': Decimal128('0.3')}}]}
{'$or': [{'INFO.0.AF_AFR': {'$gte': Decimal128('0.02')}}, {'INFO.0.AF_EUR': {'$lte': Decimal128('0.3')}}, {'INFO.0.AF_EAS': {'$gte': Decimal128('0.02')}}]}
{'$or': [{'INFO.0.AF_AFR': {'$gte': Decimal128('0.02')}}, {'INFO.0.AF_EUR': {'$gte': Decimal128('0.02')}}, {'INFO.0.AF_EAS': {'$lte': Decimal128('0.3')}}]}
```

command:
```
python3 query.py -S $HOME/Bio/pop_freq_queries -D 1000G -T $HOME/Bio/out
```

Annotate `name` column of multiple rsID-containing BED4 by dbSNP-VCF-based DB.
```
python3 annotate.py -S $HOME/Bio/UCSC -D dbSNP -T $HOME/Bio/out
```

Split the dbSNP-VCF-based DB by chromosomes, directing the results to new DB. Keep only fields from `#CHROM` to `ALT`. Index the `ID` field.
```
python3 split.py -D dbSNP -T dbSNP_min -k '#CHROM,POS,ID,REF,ALT' -i ID
```
