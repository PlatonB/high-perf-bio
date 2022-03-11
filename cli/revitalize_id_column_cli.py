__version__ = 'v2.1'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, добавляющая rsIDs в столбец ID VCF-файлов.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2021-2022
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Восстанавливаемыми файлами должны быть только сжатые VCF, а БД может содержать лишь одну
коллекцию: созданную с помощью create по VCF и содержащую rsIDs в поле ID. Рекомендуемый
источник данных для этой коллекции - NCBI dbSNP VCF с координатами подходящей сборки.

Чтобы программа работала быстро, нужен индекс полей #CHROM и POS.

Условные обозначения в справке по CLI:
[значение по умолчанию];
src-FMT - модифицируемые таблицы определённого формата;
src-db-FMT - коллекции исходной БД, соответствующие
по структуре таблицам определённого формата.
''',
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

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that adds rsIDs into ID column of VCF files.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2021-2022
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

Only compressed VCFs must be restorable files, and DB can contain only one collection:
produced by "create" from VCF and contained rsIDs in the ID field. The recommended data
source for this collection is NCBI dbSNP VCF with the coordinates of a suitable assembly.

For the program to work fast, there needs an index of #CHROM and POS fields.

The notation in the CLI help:
[default value];
src-FMT - modified tables in a certain format;
src-db-FMT - collections of source DB, matching
by structure to the tables in a certain format.
''',
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
