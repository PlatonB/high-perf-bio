__version__ = 'v4.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, позволяющая разбить каждую
коллекцию MongoDB-базы по заданному полю.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2021-2022
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Разделяемые данные должны находиться в базе, созданной с
помощью create_db или других инструментов high-perf-bio.

Чтобы программа работала быстро, нужен индекс
поля, по которому осуществляется деление.

К вложенному полю обращайтесь через точку:
"field_1.field_2.(...).field_N"

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
scr/trg-db-FMT - исходная/конечная БД с коллекциями,
соответствующими по структуре таблицам определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - сортируемые поля, а также поля,
для которых создавать составной индекс.
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Имя БД, коллекции которой разделять')
        man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                             help='Путь к папке или имя БД для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно делимых коллекций')
        opt_grp.add_argument('-f', '--spl-field-path', metavar='[None]', dest='spl_field_path', type=str,
                             help='Точечный путь к полю, по которому делить (src-db-VCF: [[#CHROM]]; src-db-BED: [[chrom]]; src-db-TSV: [[первое после _id поле]])')
        opt_grp.add_argument('-s', '--srt-field-group', metavar='[None]', dest='srt_field_group', type=str,
                             help='Точечные пути к сортируемым полям (через плюс без пробела; src-db-VCF: [[#CHROM+POS]]; src-db-BED: [[chrom+start+end]]; src-db-VCF, src-db-BED: trg-(db-)TSV)')
        opt_grp.add_argument('-o', '--srt-order', metavar='[asc]', choices=['asc', 'desc'], default='asc', dest='srt_order', type=str,
                             help='{asc, desc} Порядок сортировки (применяется с -s)')
        opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                             help='Отбираемые поля верхнего уровня (через запятую без пробела; src-db-VCF, src-db-BED: trg-(db-)TSV; поле _id не выведется)')
        opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Знак препинания для восстановления ячейки из списка (src-db-VCF, src-db-BED (trg-BED): не применяется)')
        opt_grp.add_argument('-i', '--ind-field-paths', metavar='[None]', dest='ind_field_paths', type=str,
                             help='Точечные пути к индексируемых полям (через запятую без пробела; trg-db-VCF: проиндексируются meta,#CHROM+POS,ID; trg-db-BED: <...> meta,chrom+start+end,name; trg-db-TSV: <...> meta)')
        args = arg_parser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that allows to split each collection
of the MongoDB database by specified field.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2021-2022
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

The divided data must be in the DB produced
by "create_db" or other high-perf-bio tools.

For the program to process quickly, there needs
index of the field by which the splitting is done.

Call the nested field using a point:
"field_1.field_2.(...).field_N"

The notation in the CLI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}};
scr/trg-db-FMT - source/target DB with collections,
matching by structure to the tables in a certain format;
trg-FMT - target tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error;
f1+f2+f3 - sorted fields, as well as fields,
for which to create a compound index.
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Name of the DB whose collections to split')
        man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                             help='Path to directory or name of the DB for results')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum quantity of collections splitted in parallel')
        opt_grp.add_argument('-f', '--spl-field-path', metavar='[None]', dest='spl_field_path', type=str,
                             help='Dot path to the field by which to split (src-db-VCF: [[#CHROM]]; src-db-BED: [[chrom]]; src-db-TSV: [[first field after _id]])')
        opt_grp.add_argument('-s', '--srt-field-group', metavar='[None]', dest='srt_field_group', type=str,
                             help='Dot paths to sorted fields (plus separated without spaces; src-db-VCF: [[#CHROM+POS]]; src-db-BED: [[chrom+start+end]]; src-db-VCF, src-db-BED: trg-(db-)TSV)')
        opt_grp.add_argument('-o', '--srt-order', metavar='[asc]', choices=['asc', 'desc'], default='asc', dest='srt_order', type=str,
                             help='{asc, desc} Order of sorting (applicable with -s)')
        opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                             help='Selected top level fields (comma separated without spaces; src-db-VCF, src-db-BED: trg-(db-)TSV; _id field will not be output)')
        opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Punctuation mark to restore a cell from a list (src-db-VCF, src-db-BED (trg-BED): not applicable)')
        opt_grp.add_argument('-i', '--ind-field-paths', metavar='[None]', dest='ind_field_paths', type=str,
                             help='Dot paths to indexed fields (comma separated without spaces; trg-db-VCF: meta,#CHROM+POS,ID will be indexed; trg-db-BED: meta,chrom+start+end,name <...>; trg-db-TSV: meta <...>)')
        args = arg_parser.parse_args()
        return args
