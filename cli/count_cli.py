__version__ = 'v4.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, считающая количество и, опционально, частоту
каждого значения заданного поля в пределах каждой коллекции.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Основное применение программы - определение количества и частот rsIDs.
Предполагается, что обрабатываемая коллекция была получена конкатенацией
многочисленных VCF. Разумеется, возможны и другие сценарии работы.

Обсчитываемая база должна быть создана с помощью create_db или concatenate.

К вложенному полю обращайтесь через точку:
field_1.field_2.(...).field_N

Из-за ограничений со стороны MongoDB, программа не использует индексы.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}}.
''',
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
        opt_grp.add_argument('-f', '--field-name', metavar='[None]', dest='field_name', type=str,
                             help='Имя поля, для которого считать количество каждого значения (src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[rsID]])')
        opt_grp.add_argument('-a', '--quan-thres', metavar='[1]', default=1, dest='quan_thres', type=int,
                             help='Нижняя граница количества каждого значения (применяется без -r)')
        opt_grp.add_argument('-q', '--samp-quan', metavar='[None]', dest='samp_quan', type=int,
                             help='Количество образцов (нужно для расчёта частоты каждого значения)')
        opt_grp.add_argument('-r', '--freq-thres', metavar='[None]', dest='freq_thres', type=str,
                             help='Нижняя граница частоты каждого значения (применяется с -q; применяется без -a)')
        opt_grp.add_argument('-o', '--quan-sort-order', metavar='[desc]', choices=['asc', 'desc'], default='desc', dest='quan_sort_order', type=str,
                             help='{asc, desc} Порядок сортировки по количеству каждого значения')
        args = arg_parser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that counts the quantity and, optionally, the
frequency of each value of a given field within each collection.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2021
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

The main use of the program is to determine the quantity and frequencies of
rsIDs. It is assumed that processed collection was obtained by concatenation
of multiple VCFs. Of course, other work scenarios are also possible.

The counted DB must be produced by create_db or concatenate.

Call the nested field using a point:
field_1.field_2.(...).field_N

Due to limitations on the MongoDB side, the program does not use indexes.

The notation in the CLI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}}.
''',
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
        opt_grp.add_argument('-f', '--field-name', metavar='[None]', dest='field_name', type=str,
                             help='Name of field, for which to count the quantity of each value (src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[rsID]])')
        opt_grp.add_argument('-a', '--quan-thres', metavar='[1]', default=1, dest='quan_thres', type=int,
                             help='Lower threshold of quantity of each value (applicable without -r)')
        opt_grp.add_argument('-q', '--samp-quan', metavar='[None]', dest='samp_quan', type=int,
                             help='Quantity of samples (required to calculate the frequency of each value)')
        opt_grp.add_argument('-r', '--freq-thres', metavar='[None]', dest='freq_thres', type=str,
                             help='Lower threshold of frequency of each value (applicable with -q; applicable without -a)')
        opt_grp.add_argument('-o', '--quan-sort-order', metavar='[desc]', choices=['asc', 'desc'], default='desc', dest='quan_sort_order', type=str,
                             help='{asc, desc} Order of sorting by quantity of each value')
        args = arg_parser.parse_args()
        return args
