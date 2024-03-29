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
| count_lines | outputs various metrics related to the quantity of tables rows |
| gen_test_files | creates N smaller tables from one table |

## Preparation.
### Magic installation command.
```
wget https://github.com/PlatonB/high-perf-bio/archive/refs/heads/master.zip && unzip -o master.zip && rm master.zip && cd high-perf-bio-master && bash __install_3rd_party.sh
```

- If your distro is not supported, create an [Issue](https://github.com/PlatonB/high-perf-bio/issues).
- After installation, be sure to reboot.
- Removing the toolkit dependencies is as easy as installing it: `bash __uninstall_3rd_party.sh`.
- Don't forget to check the toolkit for updates at least once a month: `bash __update_toolkit.sh`.
- If there is a `~/miniconda3` directory, the install and uninstall scripts will try to use _Conda_.

### Preparing quick GUI launch.
If the following instructions don't fit, start the GUI by `sh __run_gui_streamlit.sh` command.

#### Gnome.
Right click on `__run_gui_streamlit.sh` --> `Properties` --> `Permissions` --> `Allow executing file as program`.
After that, in the context menu will appear `Run as a Program` item.

#### KDE.
Right click on `__run_gui_streamlit.sh` --> `Properties` --> `Permissions` --> `Is executable` --> `OK`;
right click on `__run_gui_streamlit.sh` --> `Open With` --> `Other Application` --> `System` --> `Konsole` --> `OK`;
close `Konsole`.
Now it is possible to run the GUI by left-clicking on the shortcut.

### Previewing databases.
#### [MongoDB Compass](https://flathub.org/apps/details/com.mongodb.Compass).
Allows not only to visually browse DB, but also to execute queries, design aggregation pipelines, measure query performance, etc.
![MongoDB_Compass_high-perf-bio](https://user-images.githubusercontent.com/25541767/188226634-539245f2-7aed-4e11-ad6b-f587cb6cd18d.png)

#### _info_ tool.
It is a regular _high-perf-bio_ component.

List all DBs:
```
python3 info.py
```

Output to a file the most important information about a certain DB:
```
python3 info.py -d GTEx_VCF > $HOME/Bio/out/GTEx_VCF_info.txt
```

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
