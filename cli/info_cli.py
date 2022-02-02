__version__ = 'v2.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description=f'''
Программа, позволяющая вывести имена всех баз
данных или ключевую информацию об определённой БД.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Этот инструмент полезен, если вы используете Linux без графической оболочки.
Если же у вас дистрибутив с DE, предпросматривайте БД в MongoDB Compass.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]].
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-d', '--src-db-name', metavar='[None]', dest='src_db_name', type=str,
                               help='Имя БД, для которой надо вывести информацию ([[имена всех БД]])')
        argparser.add_argument('-c', '--colls-quan-limit', metavar='[5]', default=5, dest='colls_quan_limit', type=int,
                               help='Ограничение на количество характеризуемых коллекций (0 - вывести характеристики всех коллекций)')
        argparser.add_argument('-s', '--docs-quan-limit', metavar='[5]', default=5, dest='docs_quan_limit', type=int,
                               help='Ограничение на количество выводимых документов каждой коллекции (0 - вывести все документы)')
        args = argparser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description=f'''
A program that allows to print the names of
all DBs or key information about a certain DB.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2020-2021
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

This tool is good if you use Linux without a graphical shell. If you
work on a distribution with DE, you can preview DB in MongoDB Compass.

The notation in the CLI help:
[default value in the argument parsing step];
[[final default value]].
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-d', '--src-db-name', metavar='[None]', dest='src_db_name', type=str,
                               help='Name of DB for which there needs to print information ([[names of all DBs]])')
        argparser.add_argument('-c', '--colls-quan-limit', metavar='[5]', default=5, dest='colls_quan_limit', type=int,
                               help='Limit on the quantity of characterized collections (0 - print characteristics of all collections)')
        argparser.add_argument('-s', '--docs-quan-limit', metavar='[5]', default=5, dest='docs_quan_limit', type=int,
                               help='Limit on the quantity of printed documents of each collection (0 - print all documents)')
        args = argparser.parse_args()
        return args
