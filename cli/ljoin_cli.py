__version__ = 'v6.0'

from argparse import ArgumentParser, RawTextHelpFormatter
from descriptions.ljoin_descr import LjoinDescr


def add_args_ru(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=LjoinDescr(version, authors).ru,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Вывести справку и выйти')
    man_grp = arg_parser.add_argument_group('Обязательные аргументы')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Имя БД, по которой выполнять работу')
    man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                         help='Путь к папке для результатов')
    opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
    opt_grp.add_argument('-l', '--left-coll-names', metavar='[None]', dest='left_coll_names', type=str,
                         help='Имена левых коллекций (через запятую без пробела; [[все коллекции]])')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Максимальное количество параллельно обрабатываемых левых коллекций')
    opt_grp.add_argument('-r', '--right-coll-names', metavar='[None]', dest='right_coll_names', type=str,
                         help='Имена правых коллекций (через запятую без пробела; [[все коллекции]]; правая, совпадающая с текущей левой, проигнорируется)')
    opt_grp.add_argument('-q', '--extra-query', metavar="['{}']", default='{}', dest='extra_query', type=str,
                         help='Дополнительный запрос к левым коллекциям БД (в одинарных кавычках; синтаксис PyMongo; примеры указания типа данных: "any_str", Decimal128("any_str"))')
    opt_grp.add_argument('-0', '--preset', metavar='[None]', choices=['by_location', 'by_alleles'], dest='preset', type=str,
                         help='''{by_location, by_alleles} Пересекать или вычитать по геномной локации (экспериментальная фича; src-db-TSV: не применяется).
Пересекать или вычитать ID, уточняя по аллелям совпадения ID (экспериментальная фича; src-db-TSV/BED: не применяется)''')
    opt_grp.add_argument('-f', '--lookup-field-path', metavar='[None]', dest='lookup_field_path', type=str,
                         help='Точечный путь к полю, по которому пересекать или вычитать (применяется без -0; src-db-VCF: [[ID]]; src-db-BED: [[name]], src-db-TSV: [[первое после _id поле]])')
    opt_grp.add_argument('-a', '--action', metavar='[intersect]', choices=['intersect', 'subtract'], default='intersect', dest='action', type=str,
                         help='{intersect, subtract} Пересекать или вычитать')
    opt_grp.add_argument('-c', '--coverage', metavar='[1]', default=1, dest='coverage', type=int,
                         help='Охват (1 <= c <= количество правых; 0 - приравнять к количеству правых; вычтется 1, если правые и левые совпадают при 1 < c = количество правых)')
    opt_grp.add_argument('-s', '--srt-field-group', metavar='[None]', dest='srt_field_group', type=str,
                         help='Точечные пути к сортируемым полям (через плюс без пробела; src-db-VCF: [[#CHROM+POS]]; src-db-BED: [[chrom+start+end]]; src-db-VCF, src-db-BED: trg-TSV)')
    opt_grp.add_argument('-o', '--srt-order', metavar='[asc]', choices=['asc', 'desc'], default='asc', dest='srt_order', type=str,
                         help='{asc, desc} Порядок сортировки (применяется с -s)')
    opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                         help='Отбираемые поля верхнего уровня (через запятую без пробела; src-db-VCF, src-db-BED: trg-TSV; поле _id не выведется)')
    opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                         help='{colon, comma, low_line, pipe, semicolon} Знак препинания для восстановления ячейки из списка (src-db-VCF, src-db-BED (trg-BED): не применяется)')
    args = arg_parser.parse_args()
    return args


def add_args_en(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=LjoinDescr(version, authors).en,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Showing help argument')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Show this help message and exit')
    man_grp = arg_parser.add_argument_group('Mandatory arguments')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Name of the DB to work on')
    man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                         help='Path to directory for results')
    opt_grp = arg_parser.add_argument_group('Optional arguments')
    opt_grp.add_argument('-l', '--left-coll-names', metavar='[None]', dest='left_coll_names', type=str,
                         help='Left collection names (comma separated without spaces; [[all collections]])')
    opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                         help='Maximum quantity of parallel processed left collections')
    opt_grp.add_argument('-r', '--right-coll-names', metavar='[None]', dest='right_coll_names', type=str,
                         help='Right collection names (comma separated without spaces; [[all collections]]; the right one, which matches with current left one, will be ignored)')
    opt_grp.add_argument('-q', '--extra-query', metavar="['{}']", default='{}', dest='extra_query', type=str,
                         help='Additional query to left DB collections (in single quotes; PyMongo syntax; examples of specifying data type: "any_str", Decimal128("any_str"))')
    opt_grp.add_argument('-0', '--preset', metavar='[None]', choices=['by_location', 'by_alleles'], dest='preset', type=str,
                         help='''{by_location, by_alleles} Intersect or subtract by genomic location (experimental feature; src-db-TSV: not applicable).
Intersect or subtract ID, verifying by alleles ID matches (experimental feature; src-db-TSV/BED: not applicable)''')
    opt_grp.add_argument('-f', '--lookup-field-path', metavar='[None]', dest='lookup_field_path', type=str,
                         help='Dot path to the field by which to intersect or subtract (applicable without -0; src-db-VCF: [[ID]]; src-db-BED: [[name]], src-db-TSV: [[first field after _id]])')
    opt_grp.add_argument('-a', '--action', metavar='[intersect]', choices=['intersect', 'subtract'], default='intersect', dest='action', type=str,
                         help='{intersect, subtract} Intersect or subtract')
    opt_grp.add_argument('-c', '--coverage', metavar='[1]', default=1, dest='coverage', type=int,
                         help='Coverage (1 <= c <= quantity of rights; 0 - equate to the quantity of rights; it will be deducted 1 if rights and lefts match when 1 < c = quantity of rights)')
    opt_grp.add_argument('-s', '--srt-field-group', metavar='[None]', dest='srt_field_group', type=str,
                         help='Dot paths to sorted fields (plus separated without spaces; src-db-VCF: [[#CHROM+POS]]; src-db-BED: [[chrom+start+end]]; src-db-VCF, src-db-BED: trg-TSV)')
    opt_grp.add_argument('-o', '--srt-order', metavar='[asc]', choices=['asc', 'desc'], default='asc', dest='srt_order', type=str,
                         help='{asc, desc} Order of sorting (applicable with -s)')
    opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                         help='Selected top level fields (comma separated without spaces; src-db-VCF, src-db-BED: trg-TSV; _id field will not be output)')
    opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                         help='{colon, comma, low_line, pipe, semicolon} Punctuation mark to restore a cell from a list (src-db-VCF, src-db-BED (trg-BED): not applicable)')
    args = arg_parser.parse_args()
    return args
