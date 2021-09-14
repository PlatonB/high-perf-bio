__version__ = 'v2.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, позволяющая скачивать данные известных биоинформатических ресурсов.

Версия: {ver}
Требуемые сторонние компоненты: requests
Автор: Платон Быкадоров (platon.work@gmail.com), 2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

От версии к версии буду добавлять новые источники. Пишите в Issues,
какими публичными биоинформатическими данными больше всего пользуетесь.

Условные обозначения в справке по CLI:
[значение по умолчанию]
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-T', '--trg-top-dir-path', required=True, metavar='str', dest='trg_top_dir_path', type=str,
                             help='Путь к папке для скачиваемых данных')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-r', '--redownload', dest='redownload', action='store_true',
                             help='Заменить ранее скачанные данные в случае возникновения конфликта')
        opt_grp.add_argument('--cis-eqtls-gtex', dest='ciseqtls_gtex', action='store_true',
                             help='Скачать сильные пары вариант-ген (cis-eQTL, GTEx)')
        opt_grp.add_argument('--vars-1000g', dest='vars_1000g', action='store_true',
                             help='Скачать варианты 1000 Genomes (NYGC)')
        opt_grp.add_argument('--vars-dbsnp-ens', dest='vars_dbsnpens', action='store_true',
                             help='Скачать варианты dbSNP (Ensembl)')
        opt_grp.add_argument('--vars-dbsnp-ncbi', dest='vars_dbsnpncbi', action='store_true',
                             help='Скачать варианты dbSNP (NCBI)')
        opt_grp.add_argument('-i', '--download-indexes', dest='download_indexes', action='store_true',
                             help='Скачать tbi/csi-индексы, если они предоставлены')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно качаемых файлов')
        args = arg_parser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that allows to download data from well-known bioinformatic resources.

Version: {ver}
Dependencies: requests
Author: Platon Bykadorov (platon.work@gmail.com), 2021
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

I'll be adding new sources in new versions. Write to
Issues, which public bioinformatics data you use the most.

The notation in the CLI help:
[default value]
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-T', '--trg-top-dir-path', required=True, metavar='str', dest='trg_top_dir_path', type=str,
                             help='Path to the folder for downloaded data')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-r', '--redownload', dest='redownload', action='store_true',
                             help='Replace previously downloaded data in case of conflict')
        opt_grp.add_argument('--cis-eqtls-gtex', dest='ciseqtls_gtex', action='store_true',
                             help='Download significant variant-gene pairs (cis-eQTL, GTEx)')
        opt_grp.add_argument('--vars-1000g', dest='vars_1000g', action='store_true',
                             help='Download 1000 Genomes variants (NYGC)')
        opt_grp.add_argument('--vars-dbsnp-ens', dest='vars_dbsnpens', action='store_true',
                             help='Download dbSNP variants (Ensembl)')
        opt_grp.add_argument('--vars-dbsnp-ncbi', dest='vars_dbsnpncbi', action='store_true',
                             help='Download dbSNP variants (NCBI)')
        opt_grp.add_argument('-i', '--download-indexes', dest='download_indexes', action='store_true',
                             help='Download tbi/csi indexes, if they are available')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum number of files downloaded in parallel')
        args = arg_parser.parse_args()
        return args
