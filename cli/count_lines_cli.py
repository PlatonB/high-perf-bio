__version__ = 'v1.0'

import sys, os
sys.dont_write_bytecode = True
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                             'descriptions'))
from argparse import ArgumentParser, RawTextHelpFormatter
from count_lines_descr import CountLinesDescr

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=CountLinesDescr(ver).ru,
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Путь к корню дерева папок со сжатыми таблицами')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Путь к папке для конечного JSON')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Количество строк метаинформации (src-VCF: не применяется; src-BED, src-TSV: включите шапку, если она есть)')
        opt_grp.add_argument('-c', '--sel-col-nums', metavar='[None]', dest='sel_col_nums', type=str,
                             help='Номера отбираемых столбцов (через запятую без пробела; [[строка целиком]]; влияет только на результат подсчёта без дублей)')
        args = arg_parser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=CountLinesDescr(ver).en,
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Path to root of directory tree with gzipped tables')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Path to directory for target JSON')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Quantity of metainformation lines (src-VCF: not applicable; src-BED, src-TSV: include a header, if there is one)')
        opt_grp.add_argument('-c', '--sel-col-nums', metavar='[None]', dest='sel_col_nums', type=str,
                             help='Numbers of selected columns (comma separated without spaces; [[whole line]]; affects only count result without duplicates)')
        args = arg_parser.parse_args()
        return args
