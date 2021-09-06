__version__ = 'v1.1'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that allows you to run
query for all MongoDB collections.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2020-2021
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

The source of the retrieved data should be the DB produced by create_db.

For the program to work fast, it needs indexes of the fields involved in the query.

Only Python dialect of MongoDB query language is supported (see Python tab):
https://docs.mongodb.com/manual/tutorial/query-documents/

Allowed MongoDB operators:
https://docs.mongodb.com/manual/reference/operator/query/

The notation in the CLI help:
[default value];
{{permissible values}};
scr/trg-db-FMT - source/target DB with collections,
matching by structure to the tables in a certain format;
trg-FMT - target tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error;
f1+f2+f3 - fields of the DB collections with a compound index.
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Name of the DB to search in')
        man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                             help='Path to directory or name of the DB for results')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-q', '--mongo-query', metavar="['{}']", default='{}', dest='mongo_query', type=str,
                             help='Query to all DB collections (in single quotes; PyMongo syntax; examples of specifying data type: "any_str", Decimal128("any_str"))')
        opt_grp.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                             help='Selectable fields (comma separated without spaces; src-db-VCF: not applicable; src-db-BED: trg-(db-)TSV; _id field will not be output)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Punctuation mark to restore a cell from a list (src-db-VCF, src-db-BED (trg-BED): not applicable)')
        opt_grp.add_argument('-i', '--ind-field-names', metavar='[None]', dest='ind_field_names', type=str,
                             help='Names of indexable fields (comma separated without spaces; trg-db-VCF: #CHROM+POS and ID to be indexed); trg-db-BED: chrom+start+end and name to be indexed)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum number of collections to be parsed in parallel')
        args = arg_parser.parse_args()
        return args
