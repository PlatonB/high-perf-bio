__version__ = 'v1.1'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description=f'''
Программа, позволяющая вывести
имена всех баз данных или ключевую
информацию об определённой БД.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Этот инструмент создан на случай, если вы используете
Linux без графической оболочки. Если же у вас дистрибутив
с DE, предпросматривайте базы в MongoDB Compass.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]]
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
