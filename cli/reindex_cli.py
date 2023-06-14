__version__ = 'v3.1'

from argparse import ArgumentParser, RawTextHelpFormatter
from descriptions.reindex_descr import ReindexDescr


def add_args_ru(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=ReindexDescr(version, authors).ru,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Вывести справку и выйти')
    man_grp = arg_parser.add_argument_group('Обязательные аргументы')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Имя переиндексируемой БД')
    opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Максимальное количество параллельно индексируемых коллекций')
    opt_grp.add_argument('-r', '--del-ind-names', metavar='[None]', dest='del_ind_names', type=str,
                         help='Имена удаляемых индексов (через запятую без пробела)')
    opt_grp.add_argument('-a', '--ind-field-groups', metavar='[None]', dest='ind_field_groups', type=str,
                         help='Точечные пути к индексируемых полям (через запятую без пробела; для составного индекса: через плюс без пробелов)')
    args = arg_parser.parse_args()
    return args


def add_args_en(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=ReindexDescr(version, authors).en,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Showing help argument')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Show this help message and exit')
    man_grp = arg_parser.add_argument_group('Mandatory arguments')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Name of the reindexed DB')
    opt_grp = arg_parser.add_argument_group('Optional arguments')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Maximum quantity of parallel indexed collections')
    opt_grp.add_argument('-r', '--del-ind-names', metavar='[None]', dest='del_ind_names', type=str,
                         help='Names of deleted indexes (comma separated without spaces)')
    opt_grp.add_argument('-a', '--ind-field-groups', metavar='[None]', dest='ind_field_groups', type=str,
                         help='Dot paths to indexed fields (comma separated without spaces; for a compound index: plus separated without spaces)')
    args = arg_parser.parse_args()
    return args
