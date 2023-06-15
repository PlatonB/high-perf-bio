from revitalize_id_column_descr import RevitalizeIdColumnDescr
from argparse import (ArgumentParser,
                      RawTextHelpFormatter)
import sys
import os
sys.dont_write_bytecode = True
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                             'descriptions'))

__version__ = 'v3.1'


def add_args_ru(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=RevitalizeIdColumnDescr(version, authors).ru,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Вывести справку и выйти')
    man_grp = arg_parser.add_argument_group('Обязательные аргументы')
    man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                         help='Путь к папке со сжатыми исходными таблицами (src-BED и src-TSV не поддерживаются программой)')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Имя БД, содержащей коллекцию с (rs)ID-полем (src-db-BED и src-db-TSV не поддерживаются программой)')
    man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                         help='Путь к папке для результатов')
    opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Максимальное количество параллельно обогащаемых таблиц')
    opt_grp.add_argument('-r', '--ignore-unrev-lines', dest='ignore_unrev_lines', action='store_true',
                         help='Не прописывать строки, не обогащённые rsID')
    args = arg_parser.parse_args()
    return args


def add_args_en(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=RevitalizeIdColumnDescr(version, authors).en,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Showing help argument')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Show this help message and exit')
    man_grp = arg_parser.add_argument_group('Mandatory arguments')
    man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                         help='Path to directory with gzipped source tables (src-BED and src-TSV are not supported by the program)')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Name of DB containing collection with (rs)ID field (src-db-BED and src-db-TSV are not supported by the program)')
    man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                         help='Path to directory for results')
    opt_grp = arg_parser.add_argument_group('Optional arguments')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Maximum quantity of tables enriched in parallel')
    opt_grp.add_argument('-r', '--ignore-unrev-lines', dest='ignore_unrev_lines', action='store_true',
                         help="Don't write lines that not enriched by rsID")
    args = arg_parser.parse_args()
    return args
