__version__ = 'v6.0'

from argparse import ArgumentParser, RawTextHelpFormatter
from descriptions.concatenate_descr import ConcatenateDescr


def add_args_ru(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=ConcatenateDescr(version, authors).ru,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Вывести справку и выйти')
    man_grp = arg_parser.add_argument_group('Обязательные аргументы')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Имя БД, которую конкатенировать')
    man_grp.add_argument('-T', '--trg-db-name', required=True, metavar='str', dest='trg_db_name', type=str,
                         help='Имя БД для результатов')
    opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
    opt_grp.add_argument('-e', '--rewrite-existing-db', dest='rewrite_existing_db', action='store_true',
                         help='Разрешить перезаписать существующую БД в случае конфликта имён (исходную БД перезаписывать нельзя)')
    opt_grp.add_argument('-q', '--extra-query', metavar="['{}']", default='{}', dest='extra_query', type=str,
                         help='Дополнительный запрос ко всем коллекциям БД (в одинарных кавычках; синтаксис PyMongo; примеры указания типа данных: "any_str", Decimal128("any_str"))')
    opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                         help='Отбираемые поля верхнего уровня (через запятую без пробела; src-db-VCF, src-db-BED: trg-db-TSV)')
    opt_grp.add_argument('-u', '--del-copies', dest='del_copies', action='store_true',
                         help='Удалить дубли конечных документов (-k применяется ранее; вложенные структуры не поддерживаются; _id при сравнении не учитывается)')
    opt_grp.add_argument('-i', '--ind-field-groups', metavar='[None]', dest='ind_field_groups', type=str,
                         help='Точечные пути к индексируемых полям (через запятую и/или плюс без пробела; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]]; trg-db-TSV: [[первое после _id поле]])')
    args = arg_parser.parse_args()
    return args


def add_args_en(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    arg_parser = ArgumentParser(description=ConcatenateDescr(version, authors).en,
                                formatter_class=RawTextHelpFormatter,
                                add_help=False)
    hlp_grp = arg_parser.add_argument_group('Showing help argument')
    hlp_grp.add_argument('-h', '--help', action='help',
                         help='Show this help message and exit')
    man_grp = arg_parser.add_argument_group('Mandatory arguments')
    man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                         help='Name of DB to concatenate')
    man_grp.add_argument('-T', '--trg-db-name', required=True, metavar='str', dest='trg_db_name', type=str,
                         help='Name of DB for results')
    opt_grp = arg_parser.add_argument_group('Optional arguments')
    opt_grp.add_argument('-e', '--rewrite-existing-db', dest='rewrite_existing_db', action='store_true',
                         help='Allow overwriting an existing DB in case of names conflict (the source DB cannot be overwritten)')
    opt_grp.add_argument('-q', '--extra-query', metavar="['{}']", default='{}', dest='extra_query', type=str,
                         help='Additional query to all DB collections (in single quotes; PyMongo syntax; examples of specifying data type: "any_str", Decimal128("any_str"))')
    opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                         help='Selected top level fields (comma separated without spaces; src-db-VCF, src-db-BED: trg-db-TSV)')
    opt_grp.add_argument('-u', '--del-copies', dest='del_copies', action='store_true',
                         help='Remove duplicates of target documents (-k is applied previously; nested structures are not supported; _id is not taken into account when comparing)')
    opt_grp.add_argument('-i', '--ind-field-groups', metavar='[None]', dest='ind_field_groups', type=str,
                         help='Dot paths to indexed fields (comma and/or plus separated without spaces; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]]; trg-db-TSV: [[first field after _id]])')
    args = arg_parser.parse_args()
    return args
