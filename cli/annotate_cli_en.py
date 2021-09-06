__version__ = 'v1.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that retrieves the characteristics of
elements of the chosen column by MongoDB database.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2020-2021
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

Annotated column:
- must be in the same position in all source tables;
- is placed entirely in RAM, which may slow down the computer's performance.

Also as an experiment there is the possibility of intersection
by coordinates. All 4 combinations of VCF and BED are supported.

Each annotated table must be compressed using GZIP.

The source of the characteristics should be the DB produced by create_db.

For the program to work fast, it needs indexes of the fields involved in the query.

The notation in the CLI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}};
src-FMT - annotated tables in a certain format;
scr/trg-db-FMT - source/target DB with collections,
matching by structure to the tables in a certain format;
trg-FMT - target tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error
''',
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Path to directory with gzipped tables, which will be annotated')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Name of the DB by which to annotate')
        man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                             help='Path to directory or name of the DB for results')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Number of metainformation lines of annotated tables (src-VCF: not applicable; src-BED, src-TSV: include a header)')
        opt_grp.add_argument('-n', '--by-loc', dest='by_loc', action='store_true',
                             help='Intersect by genomic location (experimental feature; src-TSV, src-db-TSV: not applicable)')
        opt_grp.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                             help='Number of the annotated column (applied without -n; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
        opt_grp.add_argument('-f', '--ann-field-name', metavar='[None]', dest='ann_field_name', type=str,
                             help='Name of the DB field by which to annotate (applied without -n; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[rsID]])')
        opt_grp.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                             help='Selectable fields (comma separated without spaces; src-db-VCF: not applicable; src-db-BED: trg-(db-)TSV; _id field will not be output)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Punctuation mark to restore a cell from a list (src-db-VCF, src-db-BED (trg-BED): not applicable)')
        opt_grp.add_argument('-i', '--ind-field-names', metavar='[None]', dest='ind_field_names', type=str,
                             help='Names of indexable fields (comma separated without spaces; trg-db-VCF: #CHROM+POS and ID to be indexed); trg-db-BED: chrom+start+end and name to be indexed)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum number of tables to be annotated in parallel')
        args = arg_parser.parse_args()
        return args
