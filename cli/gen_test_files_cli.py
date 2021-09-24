__version__ = 'v2.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, создающая наборы тестировочных файлов из случайных строк исходного.

Версия: {ver}
Требуемые сторонние компоненты: -
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Исходный файл должен быть сжат с помощью GZIP.

Вероятность попадания строки в конечный файл = 1 / жёсткость прореживания.

Конечные файлы будут схожи по размеру и частично совпадать по составу строк.

Условные обозначения в справке по CLI:
[значение по умолчанию];
src-FMT - разреживаемая таблица определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку.
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-file-path', required=True, metavar='str', dest='src_file_path', type=str,
                             help='Путь к сжатому файлу')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Путь к папке для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Количество строк метаинформации (src-VCF: не применяется; src-BED, src-TSV: включите шапку)')
        opt_grp.add_argument('-r', '--thinning-lvl', metavar='[10]', default=10, dest='thinning_lvl', type=int,
                             help='Жёсткость прореживания (чем она больше, тем меньше останется строк)')
        opt_grp.add_argument('-n', '--trg-files-quan', metavar='[4]', default=4, dest='trg_files_quan', type=int,
                             help='Количество файлов, по которым разнести результаты')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно генерируемых файлов')
        args = arg_parser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that creates sets of test files from random strings of the source file.

Version: {ver}
Dependencies: -
Author: Platon Bykadorov (platon.work@gmail.com), 2020-2021
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

The source file must be compressed by GZIP.

Probability of hitting a line into the target file = 1 / thinning level.

The target files will be similar in size and partially overlap in the content.

The notation in the CLI help:
[default value];
src-FMT - thinned table in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error.
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-S', '--src-file-path', required=True, metavar='str', dest='src_file_path', type=str,
                             help='Path to gzipped file')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Path to directory for results')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Quantity of metainformation lines (src-VCF: not applicable; src-BED, src-TSV: include a header)')
        opt_grp.add_argument('-r', '--thinning-lvl', metavar='[10]', default=10, dest='thinning_lvl', type=int,
                             help='Thinning level (the higher it is, the fewer lines will be remain)')
        opt_grp.add_argument('-n', '--trg-files-quan', metavar='[4]', default=4, dest='trg_files_quan', type=int,
                             help='Quantity of files by which to distribute the results')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum quantity of files generated in parallel')
        args = arg_parser.parse_args()
        return args
