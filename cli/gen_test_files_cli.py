__version__ = 'v1.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, создающая наборы тестировочных
файлов из случайных строк исходного.

Версия: {ver}
Требуемые сторонние компоненты: -
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Исходный файл должен быть сжат с помощью GZIP.

Вероятность попадания строки в конечный
файл = 1 / жёсткость прореживания.

Конечные файлы будут схожи по размеру
и частично совпадать по составу строк.

Условные обозначения в справке по CLI:
[значение по умолчанию];
src-FMT - разреживаемая таблица определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку
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
