from argparse import (ArgumentParser,
                      RawTextHelpFormatter)
from descriptions.info_descr import InfoDescr

__version__ = 'v3.2'


def add_args_ru(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    argparser = ArgumentParser(description=InfoDescr(version, authors).ru,
                               formatter_class=RawTextHelpFormatter)
    argparser.add_argument('-d', '--src-db-name', metavar='[None]', dest='src_db_name', type=str,
                           help='Имя БД, для которой надо вывести информацию ([[имена всех БД]])')
    argparser.add_argument('-c', '--colls-quan-limit', metavar='[5]', default=5, dest='colls_quan_limit', type=int,
                           help='Ограничение на количество характеризуемых коллекций (0 - вывести характеристики всех коллекций)')
    argparser.add_argument('-s', '--docs-quan-limit', metavar='[5]', default=5, dest='docs_quan_limit', type=int,
                           help='Ограничение на количество выводимых документов каждой коллекции (0 - вывести все документы)')
    args = argparser.parse_args()
    return args


def add_args_en(version, authors):
    '''
    Работа с аргументами командной строки.
    '''
    argparser = ArgumentParser(description=InfoDescr(version, authors).en,
                               formatter_class=RawTextHelpFormatter)
    argparser.add_argument('-d', '--src-db-name', metavar='[None]', dest='src_db_name', type=str,
                           help='Name of DB for which there needs to print information ([[names of all DBs]])')
    argparser.add_argument('-c', '--colls-quan-limit', metavar='[5]', default=5, dest='colls_quan_limit', type=int,
                           help='Limit on the quantity of characterized collections (0 - print characteristics of all collections)')
    argparser.add_argument('-s', '--docs-quan-limit', metavar='[5]', default=5, dest='docs_quan_limit', type=int,
                           help='Limit on the quantity of printed documents of each collection (0 - print all documents)')
    args = argparser.parse_args()
    return args
