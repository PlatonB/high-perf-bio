__version__ = 'v1.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that can delete existing indexes of MongoDB database and add new ones.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2020-2021
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

Do not confuse the concepts of field name and index name.

To print names of databases, indexes and fields I recommend
to use print_db_info from high-perf-bio or MongoDB Compass.

The creation/deletion of both single and compound indexes is supported.

The notation in the CLI help:
[default value]
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-D', '--db-name', required=True, metavar='str', dest='db_name', type=str,
                             help='Name of the reindexed DB')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-r', '--del-ind-names', metavar='[None]', dest='del_ind_names', type=str,
                             help='Names of deleted indexes (comma separated without spaces)')
        opt_grp.add_argument('-a', '--ind-field-names', metavar='[None]', dest='ind_field_names', type=str,
                             help='Names of indexed fields (comma separated without spaces; for a compound index: plus separated without spaces)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum number of parallel indexed collections')
        args = arg_parser.parse_args()
        return args
