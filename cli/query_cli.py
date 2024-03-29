from argparse import (ArgumentParser,
                      RawTextHelpFormatter)
from descriptions.query_descr import QueryDescr

__version__ = 'v7.2'


def add_args_ru(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=QueryDescr(version, authors).ru,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Вывести справку и выйти')
    man_grp = arg_parser.add_argument_group('Обязательные аргументы')
    man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                         help='Путь к папке с содержащими запросы файлами')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Имя БД, по которой искать')
    man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                         help='Путь к папке или имя БД для результатов')
    opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Максимальное количество параллельно считываемых файлов с запросами')
    opt_grp.add_argument('-e', '--rewrite-existing-db', dest='rewrite_existing_db', action='store_true',
                         help='Разрешить перезаписать существующую БД в случае конфликта имён (исходную БД перезаписывать нельзя)')
    opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                         help='Количество строк метаинформации файлов с запросами')
    opt_grp.add_argument('-s', '--srt-field-group', metavar='[None]', dest='srt_field_group', type=str,
                         help='Точечные пути к сортируемым полям (через плюс без пробела; src-db-VCF, src-db-BED: trg-(db-)TSV)')
    opt_grp.add_argument('-o', '--srt-order', metavar='[asc]', choices=['asc', 'desc'], default='asc', dest='srt_order', type=str,
                         help='{asc, desc} Порядок сортировки (применяется с -s)')
    opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                         help='Отбираемые поля верхнего уровня (через запятую без пробела; src-db-VCF, src-db-BED: trg-(db-)TSV; поле _id не выведется)')
    opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                         help='{colon, comma, low_line, pipe, semicolon} Знак препинания для восстановления ячейки из списка (src-db-VCF, src-db-BED (trg-BED): не применяется)')
    opt_grp.add_argument('-i', '--ind-field-groups', metavar='[None]', dest='ind_field_groups', type=str,
                         help='Точечные пути к индексируемых полям (через запятую и/или плюс без пробела; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]]; trg-db-TSV: [[первое после _id поле]])')
    args = arg_parser.parse_args()
    return args


def add_args_en(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=QueryDescr(version, authors).en,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Showing help argument')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Show this help message and exit')
    man_grp = arg_parser.add_argument_group('Mandatory arguments')
    man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                         help='Path to directory with files containing queries')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Name of the DB to search in')
    man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                         help='Path to directory or name of the DB for results')
    opt_grp = arg_parser.add_argument_group('Optional arguments')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Maximum quantity of files with queries read in parallel')
    opt_grp.add_argument('-e', '--rewrite-existing-db', dest='rewrite_existing_db', action='store_true',
                         help='Allow overwriting an existing DB in case of names conflict (the source DB cannot be overwritten)')
    opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                         help='Quantity of metainformation lines of files with queries')
    opt_grp.add_argument('-s', '--srt-field-group', metavar='[None]', dest='srt_field_group', type=str,
                         help='Dot paths to sorted fields (plus separated without spaces; src-db-VCF, src-db-BED: trg-(db-)TSV)')
    opt_grp.add_argument('-o', '--srt-order', metavar='[asc]', choices=['asc', 'desc'], default='asc', dest='srt_order', type=str,
                         help='{asc, desc} Order of sorting (applicable with -s)')
    opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                         help='Selected top level fields (comma separated without spaces; src-db-VCF, src-db-BED: trg-(db-)TSV; _id field will not be output)')
    opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                         help='{colon, comma, low_line, pipe, semicolon} Punctuation mark to restore a cell from a list (src-db-VCF, src-db-BED (trg-BED): not applicable)')
    opt_grp.add_argument('-i', '--ind-field-groups', metavar='[None]', dest='ind_field_groups', type=str,
                         help='Dot paths to indexed fields (comma and/or plus separated without spaces; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]]; trg-db-TSV: [[first field after _id]])')
    args = arg_parser.parse_args()
    return args
