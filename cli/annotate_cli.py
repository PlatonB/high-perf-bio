from argparse import (ArgumentParser,
                      RawTextHelpFormatter)
from descriptions.annotate_descr import AnnotateDescr

__version__ = 'v8.1'


def add_args_ru(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=AnnotateDescr(version, authors).ru,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Вывести справку и выйти')
    man_grp = arg_parser.add_argument_group('Обязательные аргументы')
    man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                         help='Путь к папке со сжатыми аннотируемыми таблицами')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Имя БД, по которой аннотировать')
    man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                         help='Путь к папке или имя БД для результатов')
    opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Максимальное количество параллельно аннотируемых таблиц')
    opt_grp.add_argument('-e', '--rewrite-existing-db', dest='rewrite_existing_db', action='store_true',
                         help='Разрешить перезаписать существующую БД в случае конфликта имён (исходную БД перезаписывать нельзя)')
    opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                         help='Количество строк метаинформации аннотируемых таблиц (src-VCF: не применяется; src-BED, src-TSV: включите шапку)')
    opt_grp.add_argument('-q', '--extra-query', metavar="['{}']", default='{}', dest='extra_query', type=str,
                         help='''Дополнительный запрос ко всем коллекциям БД (в одинарных кавычках; синтаксис PyMongo;
{"$and": [ваш_запрос]} при необходимости экранирования имени запрашиваемого поля или оператора $or;
примеры указания типа данных: "any_str", Decimal128("any_str"))''')
    opt_grp.add_argument('-0', '--preset', metavar='[None]', choices=['by_location', 'by_alleles'], dest='preset', type=str,
                         help='''{by_location, by_alleles} Аннотировать, пересекая по геномной локации (экспериментальная фича; src-TSV, src-db-TSV: не применяется).
Аннотировать ID по ID, уточняя по аллелям совпадения ID (экспериментальная фича; src-TSV/BED, src-db-TSV/BED: не применяется)''')
    opt_grp.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                         help='Номер аннотируемого столбца (применяется без -0; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
    opt_grp.add_argument('-f', '--ann-field-path', metavar='[None]', dest='ann_field_path', type=str,
                         help='Точечный путь к полю, по которому аннотировать (применяется без -0; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[первое после _id поле]])')
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
    arg_parser = ArgumentParser(description=AnnotateDescr(version, authors).en,
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
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Maximum quantity of parallel annotated tables')
    opt_grp.add_argument('-e', '--rewrite-existing-db', dest='rewrite_existing_db', action='store_true',
                         help='Allow overwriting an existing DB in case of names conflict (the source DB cannot be overwritten)')
    opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                         help='Quantity of metainformation lines of annotated tables (src-VCF: not applicable; src-BED, src-TSV: include a header)')
    opt_grp.add_argument('-q', '--extra-query', metavar="['{}']", default='{}', dest='extra_query', type=str,
                         help='''Additional query to all DB collections (in single quotes; PyMongo syntax;
{"$and": [your_query]} if necessary to shield the queried field name or $or operator;
examples of specifying data type: "any_str", Decimal128("any_str"))''')
    opt_grp.add_argument('-0', '--preset', metavar='[None]', choices=['by_location', 'by_alleles'], dest='preset', type=str,
                         help='''{by_location, by_alleles} Annotate via intersection by genomic location (experimental feature; src-TSV, src-db-TSV: not applicable).
Annotate ID by ID, verifying by alleles ID matches (experimental feature; src-TSV/BED, src-db-TSV/BED: not applicable)''')
    opt_grp.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                         help='Number of the annotated column (applicable without -0; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
    opt_grp.add_argument('-f', '--ann-field-path', metavar='[None]', dest='ann_field_path', type=str,
                         help='Dot path to the field by which to annotate (applicable without -0; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[first field after _id]])')
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
