__version__ = 'v7.1'

from argparse import ArgumentParser, RawTextHelpFormatter
from descriptions.count_descr import CountDescr

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=CountDescr(ver).ru,
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Имя анализируемой БД')
        man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                             help='Путь к папке или имя БД для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-e', '--rewrite-existing-db', dest='rewrite_existing_db', action='store_true',
                             help='Разрешить перезаписать существующую БД в случае конфликта имён (исходную БД перезаписывать нельзя)')
        opt_grp.add_argument('-f', '--cnt-field-paths', metavar='[None]', dest='cnt_field_paths', type=str,
                             help='''Точечные пути к полям, для которых считать количество каждого набора взаимосвязанных значений (через запятую без пробела;
src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[первое после _id поле]])''')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[low_line]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='low_line', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Знак препинания для восстановления ячейки из списка')
        opt_grp.add_argument('-a', '--quan-thres', metavar='[1]', default=1, dest='quan_thres', type=int,
                             help='Нижняя граница количества каждого набора (применяется без -r)')
        opt_grp.add_argument('-q', '--samp-quan', metavar='[None]', dest='samp_quan', type=int,
                             help='Количество образцов (нужно для расчёта частоты каждого набора)')
        opt_grp.add_argument('-r', '--freq-thres', metavar='[None]', dest='freq_thres', type=str,
                             help='Нижняя граница частоты каждого набора (применяется с -q; применяется без -a)')
        opt_grp.add_argument('-o', '--quan-sort-order', metavar='[desc]', choices=['asc', 'desc'], default='desc', dest='quan_sort_order', type=str,
                             help='{asc, desc} Порядок сортировки по количеству каждого набора')
        args = arg_parser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=CountDescr(ver).en,
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Name of the analyzed DB')
        man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                             help='Path to directory or name of the DB for results')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-e', '--rewrite-existing-db', dest='rewrite_existing_db', action='store_true',
                             help='Allow overwriting an existing DB in case of names conflict (the source DB cannot be overwritten)')
        opt_grp.add_argument('-f', '--cnt-field-paths', metavar='[None]', dest='cnt_field_paths', type=str,
                             help='''Dot paths to fields for which to count the quantity of each set of related values (comma separated without spaces;
src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[first field after _id]])''')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[low_line]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='low_line', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Punctuation mark to restore a cell from a list')
        opt_grp.add_argument('-a', '--quan-thres', metavar='[1]', default=1, dest='quan_thres', type=int,
                             help='Lower threshold of quantity of each set (applicable without -r)')
        opt_grp.add_argument('-q', '--samp-quan', metavar='[None]', dest='samp_quan', type=int,
                             help='Quantity of samples (required to calculate the frequency of each set)')
        opt_grp.add_argument('-r', '--freq-thres', metavar='[None]', dest='freq_thres', type=str,
                             help='Lower threshold of frequency of each set (applicable with -q; applicable without -a)')
        opt_grp.add_argument('-o', '--quan-sort-order', metavar='[desc]', choices=['asc', 'desc'], default='desc', dest='quan_sort_order', type=str,
                             help='{asc, desc} Order of sorting by quantity of each set')
        args = arg_parser.parse_args()
        return args
