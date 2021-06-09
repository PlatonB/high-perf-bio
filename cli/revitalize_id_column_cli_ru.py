__version__ = 'v1.1'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, добавляющая rsIDs в столбец ID VCF-файлов.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Восстанавливаемыми файлами должны быть только сжатые VCF, а БД может содержать лишь одну
коллекцию: созданную с помощью create_db по VCF и содержащую rsIDs в поле ID. Рекомендуемый
источник данных для этой коллекции - NCBI dbSNP VCF с координатами подходящей сборки.

Чтобы программа работала быстро, нужен индекс полей #CHROM и POS.

Условные обозначения в справке по CLI:
[значение по умолчанию];
src-FMT - модифицируемые таблицы определённого формата;
scr-db-FMT - исходная БД с коллекциями, соответствующими
по структуре таблицам определённого формата
''',
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Путь к папке со сжатыми исходными таблицами (src-BED, src-TSV: не поддерживаются')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Имя БД, содержащей коллекцию с (rs)ID-полем (src-db-BED, src-db-TSV: не поддерживаются)')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Путь к папке для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-r', '--ignore-unrev-lines', dest='ignore_unrev_lines', action='store_true',
                             help='Не прописывать строки, не обогащённые rsID')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно обогащаемых таблиц')
        args = arg_parser.parse_args()
        return args
