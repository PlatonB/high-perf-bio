from argparse import (ArgumentParser,
                      RawTextHelpFormatter)
from descriptions.create_descr import CreateDescr

__version__ = 'v7.0'


def add_args_ru(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=CreateDescr(version, authors).ru,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Вывести справку и выйти')
    man_grp = arg_parser.add_argument_group('Обязательные аргументы')
    man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                         help='Путь к папке со сжатыми таблицами, преобразуемыми в коллекции MongoDB-базы')
    opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
    opt_grp.add_argument('-d', '--trg-db-name', metavar='[None]', dest='trg_db_name', type=str,
                         help='Имя создаваемой базы данных ([[имя папки со сжатыми таблицами]])')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Максимальное количество параллельно загружаемых таблиц/индексируемых коллекций')
    opt_grp.add_argument('-e', '--if-db-exists', metavar='[None]', choices=['rewrite', 'replenish'], dest='if_db_exists', type=str,
                         help='{rewrite, replenish} Стереть имеющуюся БД или пополнить её данными из новых файлов папки (не допускайте смешивания форматов в одной БД)')
    opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                         help='Количество строк метаинформации (src-VCF: не применяется; src-BED: включите шапку (если есть); src-TSV: не включайте шапку (если есть), либо применяйте -n)')
    opt_grp.add_argument('-n', '--arbitrary-header', metavar='[None]', dest='arbitrary_header', type=str,
                         help='Произвольная шапка исходных таблиц (в кавычках; через \\t без пробела; src-VCF, src-BED: не применяется)')
    opt_grp.add_argument('-2', '--dbsnp2', dest='dbsnp2', action='store_true',
                         help='Упрощать, насколько это возможно, INFO-ячейки dbSNP 2.0 src-VCF (src-BED, src-TSV: не применяется)')
    opt_grp.add_argument('-r', '--minimal', dest='minimal', action='store_true',
                         help='Загружать только минимально допустимый форматом набор столбцов (src-VCF: 1-ые 8; src-BED: 1-ые 3; src-TSV: не применяется)')
    opt_grp.add_argument('-,', '--sec-delimiter', metavar='[None]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], dest='sec_delimiter', type=str,
                         help='{colon, comma, low_line, pipe, semicolon} Знак препинания для разбиения ячейки на список (src-VCF, src-BED: не применяется)')
    opt_grp.add_argument('-c', '--max-fragment-len', metavar='[100000]', default=100000, dest='max_fragment_len', type=int,
                         help='Максимальное количество строк фрагмента заливаемой таблицы')
    opt_grp.add_argument('-i', '--ind-field-groups', metavar='[None]', dest='ind_field_groups', type=str,
                         help='Точечные пути к индексируемых полям (через запятую и/или плюс без пробела; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]])')
    args = arg_parser.parse_args()
    return args


def add_args_en(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=CreateDescr(version, authors).en,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Showing help argument')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Show this help message and exit')
    man_grp = arg_parser.add_argument_group('Mandatory arguments')
    man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                         help='Path to directory with gzipped tables which will be converted to collections of MongoDB database')
    opt_grp = arg_parser.add_argument_group('Optional arguments')
    opt_grp.add_argument('-d', '--trg-db-name', metavar='[None]', dest='trg_db_name', type=str,
                         help='Name of the created DB ([[compressed tables directory name]])')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Maximum quantity of parallel uploaded tables/indexed collections')
    opt_grp.add_argument('-e', '--if-db-exists', metavar='[None]', choices=['rewrite', 'replenish'], dest='if_db_exists', type=str,
                         help='{rewrite, replenish} Erase the target DB or replenish it with data from new files of the directory (do not allow mixing of formats in the same DB)')
    opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                         help='Quantity of metainformation lines (src-VCF: not applicable; src-BED: include a header (if available); src-TSV: do not include a header (if available), otherwise use -n)')
    opt_grp.add_argument('-n', '--arbitrary-header', metavar='[None]', dest='arbitrary_header', type=str,
                         help='Arbitrary header of source tables (in quotes; \\t-separated without spaces; src-VCF, src-BED: not applicable)')
    opt_grp.add_argument('-2', '--dbsnp2', dest='dbsnp2', action='store_true',
                         help='Simplify, as much as possible, INFO cells of dbSNP 2.0 src-VCF (src-BED, src-TSV: not applicable)')
    opt_grp.add_argument('-r', '--minimal', dest='minimal', action='store_true',
                         help='Upload only the minimum set of columns allowed by the format (src-VCF: 1st 8; src-BED: 1st 3; src-TSV: not applicable)')
    opt_grp.add_argument('-,', '--sec-delimiter', metavar='[None]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], dest='sec_delimiter', type=str,
                         help='{colon, comma, low_line, pipe, semicolon} Punctuation mark for dividing a cell to a list (src-VCF, src-BED: not applicable)')
    opt_grp.add_argument('-c', '--max-fragment-len', metavar='[100000]', default=100000, dest='max_fragment_len', type=int,
                         help='Maximum quantity of rows of uploaded table fragment')
    opt_grp.add_argument('-i', '--ind-field-groups', metavar='[None]', dest='ind_field_groups', type=str,
                         help='Dot paths to indexed fields (comma and/or plus separated without spaces; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]])')
    args = arg_parser.parse_args()
    return args
