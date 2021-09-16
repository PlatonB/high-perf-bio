__version__ = 'v1.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, считающая количество каждого значения
заданного поля в пределах каждой коллекции.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Обсчитываемая база должна быть создана с помощью create_db.

К вложенному полю обращайтесь через точку:
field_1.field_2.(...).field_N

Из-за ограничений со стороны MongoDB, программа не использует индексы.

Условные обозначения в справке по CLI:
[значение по умолчанию];
{{допустимые значения}}
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
        man_grp.add_argument('-F', '--field-name', required=True, metavar='str', dest='field_name', type=str,
                             help='Имя поля коллекций БД, для которого считать количество каждого значения')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-b', '--quan-thres', metavar='[1]', default=1, dest='quan_thres', type=int,
                             help='Нижняя граница количества')
        opt_grp.add_argument('-o', '--quan-sort-order', metavar='[desc]', choices=['asc', 'desc'], default='desc', dest='quan_sort_order', type=str,
                             help='{asc, desc} Порядок сортировки по количеству')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно обрабатываемых коллекций')
        args = arg_parser.parse_args()
        return args
