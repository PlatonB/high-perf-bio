__version__ = 'v1.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, выполняющая пересечение
или вычитание коллекций по выбранному
полю или по геномным координатам.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Программу можно применять только для
баз, созданных с помощью create_db.

--------------------------------------------------

Вам необходимо условно отнести интересующие
коллекции к категориям "левые" и "правые".

Левые: в конечные файлы попадают
данные только из них. Одна левая
коллекция - один файл с результатами.

Правые: нужны для фильтрации левых.

--------------------------------------------------

Пересечение по одному полю.

Указанное поле *каждой* левой коллекции пересекается
с одноимённым полем *всех* правых коллекций.

Как работает настройка охвата пересечения?
*Остаются* только те значения поля левой коллекции,
для которых *есть совпадение* в соответствующем
поле, как минимум, того количества правых
коллекций, что задано параметром охвата.

--------------------------------------------------

Вычитание по одному полю.

Из указанного поля *каждой* левой коллекции
вычитается одноимённое поле *всех* правых коллекций.

Как работает настройка охвата вычитания?
*Остаются* только те значения поля левой коллекции,
для которых *нет совпадения* в соответствующем
поле, как минимум, того количества правых
коллекций, что задано параметром охвата.

--------------------------------------------------

Об охвате простым языком.

Больше охват - меньше результатов.

--------------------------------------------------

Пересечение и вычитание по геномной локации.
- актуальны все написанные выше разъяснения,
касающиеся работы с единичным полем;
- src-db-BED: стартовая координата каждого
интервала - 0-based, т.е. равна
истинному номеру нуклеотида минус 1;
- src-db-BED: левые интервалы попадают
в результаты в неизменном виде.
- src-db-BED: баг - неприлично низкая
скорость вычислений (Issue #7);

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-db-FMT - исходная БД с коллекциями, соответствующими
по структуре таблицам определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку.
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Имя БД, по которой выполнять работу')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Путь к папке для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-l', '--left-coll-names', metavar='[None]', dest='left_coll_names', type=str,
                             help='Имена левых коллекций (через запятую без пробела; [[все коллекции БД]])')
        opt_grp.add_argument('-r', '--right-coll-names', metavar='[None]', dest='right_coll_names', type=str,
                             help='Имена правых коллекций (через запятую без пробела; [[все коллекции БД]]; правая, совпадающая с текущей левой, проигнорируется)')
        opt_grp.add_argument('-n', '--by-loc', dest='by_loc', action='store_true',
                             help='Пересекать или вычитать по геномной локации (экспериментальная фича; src-db-TSV: не применяется)')
        opt_grp.add_argument('-f', '--field-name', metavar='[None]', dest='field_name', type=str,
                             help='Имя поля, по которому пересекать или вычитать (применяется без -n; src-db-VCF: [[ID]]; src-db-BED: [[name]], src-db-TSV: [[rsID]])')
        opt_grp.add_argument('-a', '--action', metavar='[intersect]', choices=['intersect', 'subtract'], default='intersect', dest='action', type=str,
                             help='{intersect, subtract} Пересекать или вычитать')
        opt_grp.add_argument('-c', '--coverage', metavar='[1]', default=1, dest='coverage', type=int,
                             help='Охват (1 <= c <= количество правых; 0 - приравнять к количеству правых; вычтется 1, если правые и левые совпадают при 1 < c = количество правых)')
        opt_grp.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                             help='Отбираемые поля (через запятую без пробела; src-db-VCF: не применяется; src-db-BED: trg-TSV; поле _id не выведется)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Знак препинания для восстановления ячейки из списка (src-db-VCF, src-db-BED (trg-BED): не применяется)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно обрабатываемых левых коллекций')
        args = arg_parser.parse_args()
        return args
