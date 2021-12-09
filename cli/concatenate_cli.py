__version__ = 'v2.2'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, объединяющая все коллекции
одной MongoDB-базы с выводом в другую.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Конкатенируемая база должна быть создана с помощью
create_db или других инструментов high-perf-bio.

Набор полей объединяемых коллекций должен быть одинаковым.

Условные обозначения в справке по CLI:
[значение по умолчанию];
scr/trg-db-FMT - исходная/конечная БД с коллекциями,
соответствующими по структуре таблицам определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - поля коллекций БД с составным индексом.
''',
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
        opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                             help='Отбираемые поля верхнего уровня (через запятую без пробела; src-db-VCF, src-db-BED: trg-db-TSV)')
        opt_grp.add_argument('-u', '--del-copies', dest='del_copies', action='store_true',
                             help='Удалить дубли конечных документов (-k применяется ранее; вложенные структуры не поддерживаются; _id при сравнении не учитывается)')
        opt_grp.add_argument('-i', '--ind-field-paths', metavar='[None]', dest='ind_field_paths', type=str,
                             help='Точечные пути к индексируемых полям (через запятую без пробела; trg-db-VCF: проиндексируются meta,#CHROM+POS,ID; trg-db-BED: <...> meta,chrom+start+end,name; trg-db-TSV: <...> meta)')
        args = arg_parser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that merges all collections of
one MongoDB database with output to another.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2021
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

Concatenated DB must be produced by create_db or other high-perf-bio tools.

The set of fields of merged collections must be the same.

The notation in the CLI help:
[default value];
scr/trg-db-FMT - source/target DB with collections,
matching by structure to the tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error;
f1+f2+f3 - fields of the DB collections with a compound index
''',
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
        opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                             help='Selected top level fields (comma separated without spaces; src-db-VCF, src-db-BED: trg-db-TSV)')
        opt_grp.add_argument('-u', '--del-copies', dest='del_copies', action='store_true',
                             help='Remove duplicates of target documents (-k is applied previously; nested structures are not supported; _id is not taken into account when comparing)')
        opt_grp.add_argument('-i', '--ind-field-paths', metavar='[None]', dest='ind_field_paths', type=str,
                             help='Dot paths to indexed fields (comma separated without spaces; trg-db-VCF: meta,#CHROM+POS,ID will be indexed; trg-db-BED: meta,chrom+start+end,name <...>; trg-db-TSV: meta <...>)')
        args = arg_parser.parse_args()
        return args
