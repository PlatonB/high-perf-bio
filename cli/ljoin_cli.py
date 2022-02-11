__version__ = 'v3.0'

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
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2022
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Программу можно применять только для баз, созданных с
помощью create или других инструментов high-perf-bio.

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
в результаты в неизменном виде;
- src-db-BED: баг - неприлично низкая
скорость вычислений (Issue #7).

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-db-FMT - коллекции исходной БД, соответствующие
по структуре таблицам определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - сортируемые поля.
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
                             help='Имена левых коллекций (через запятую без пробела; [[все коллекции]])')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно обрабатываемых левых коллекций')
        opt_grp.add_argument('-r', '--right-coll-names', metavar='[None]', dest='right_coll_names', type=str,
                             help='Имена правых коллекций (через запятую без пробела; [[все коллекции]]; правая, совпадающая с текущей левой, проигнорируется)')
        opt_grp.add_argument('-n', '--by-loc', dest='by_loc', action='store_true',
                             help='Пересекать или вычитать по геномной локации (экспериментальная фича; src-db-TSV: не применяется)')
        opt_grp.add_argument('-f', '--lookup-field-path', metavar='[None]', dest='lookup_field_path', type=str,
                             help='Точечный путь к полю, по которому пересекать или вычитать (применяется без -n; src-db-VCF: [[ID]]; src-db-BED: [[name]], src-db-TSV: [[первое после _id поле]])')
        opt_grp.add_argument('-a', '--action', metavar='[intersect]', choices=['intersect', 'subtract'], default='intersect', dest='action', type=str,
                             help='{intersect, subtract} Пересекать или вычитать')
        opt_grp.add_argument('-c', '--coverage', metavar='[1]', default=1, dest='coverage', type=int,
                             help='Охват (1 <= c <= количество правых; 0 - приравнять к количеству правых; вычтется 1, если правые и левые совпадают при 1 < c = количество правых)')
        opt_grp.add_argument('-s', '--srt-field-group', metavar='[None]', dest='srt_field_group', type=str,
                             help='Точечные пути к сортируемым полям (через плюс без пробела; src-db-VCF: [[#CHROM+POS]]; src-db-BED: [[chrom+start+end]]; src-db-VCF, src-db-BED: trg-TSV)')
        opt_grp.add_argument('-o', '--srt-order', metavar='[asc]', choices=['asc', 'desc'], default='asc', dest='srt_order', type=str,
                             help='{asc, desc} Порядок сортировки (применяется с -s)')
        opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                             help='Отбираемые поля верхнего уровня (через запятую без пробела; src-db-VCF, src-db-BED: trg-TSV; поле _id не выведется)')
        opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Знак препинания для восстановления ячейки из списка (src-db-VCF, src-db-BED (trg-BED): не применяется)')
        args = arg_parser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that performs intersection or subtraction of
collections by chosen field or by genomic coordinates.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2020-2022
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

The program can be used only for DBs produced
by "create" or other high-perf-bio tools.

--------------------------------------------------

You need conditionally classify interesting
collections into "lefts" and "rights" categories.

Lefts: only the data from them will
be included in the target files. One
left collection - one file with results.

Rights: needed to filter the lefts.

--------------------------------------------------

Intersection by one field.

The specified field of *each* left collection intersects
with the same field of *all* right collections.

How does the intersection coverage setting work?
*Remain* only those values of the left collection
field for which *there is a match* in the corresponding
field of at least that quantity of right collections
that is specified by the coverage setting.

--------------------------------------------------

Subtraction by one field.

From the specified field of *each* left collection
subtracted the same field of *all* right collections.

How does the subtraction coverage setting work?
*Remain* only those values of the left collection
field for which *there is no match* in the corresponding
field of at least that quantity of right collections
that is specified by the coverage setting.

--------------------------------------------------

About coverage in simple terms.

More coverage means less results.

--------------------------------------------------

Intersection and subtraction by genomic location.
- all the explanations written above regarding
working with a single field are relevant;
- src-db-BED: the starting coordinate
of each interval is 0-based, i.e., equal
to real nucleotide number minus 1;
- src-db-BED: left intervals arrive
into the results in unchanged form;
- src-db-BED: bug: obscenely low
calculation speed (Issue #7).

The notation in the CLI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}};
scr-db-FMT - collections of source DB, matching
by structure to the tables in a certain format;
trg-FMT - target tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error.
f1+f2+f3 - sorted fields.
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Name of the DB to work on')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Path to directory for results')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-l', '--left-coll-names', metavar='[None]', dest='left_coll_names', type=str,
                             help='Left collection names (comma separated without spaces; [[all collections]])')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum quantity of parallel processed left collections')
        opt_grp.add_argument('-r', '--right-coll-names', metavar='[None]', dest='right_coll_names', type=str,
                             help='Right collection names (comma separated without spaces; [[all collections]]; the right one, which matches with current left one, will be ignored)')
        opt_grp.add_argument('-n', '--by-loc', dest='by_loc', action='store_true',
                             help='Intersect or subtract by genomic location (experimental feature; src-db-TSV: not applicable)')
        opt_grp.add_argument('-f', '--lookup-field-path', metavar='[None]', dest='lookup_field_path', type=str,
                             help='Dot path to the field by which to intersect or subtract (applied without -n; src-db-VCF: [[ID]]; src-db-BED: [[name]], src-db-TSV: [[first field after _id]])')
        opt_grp.add_argument('-a', '--action', metavar='[intersect]', choices=['intersect', 'subtract'], default='intersect', dest='action', type=str,
                             help='{intersect, subtract} Intersect or subtract')
        opt_grp.add_argument('-c', '--coverage', metavar='[1]', default=1, dest='coverage', type=int,
                             help='Coverage (1 <= c <= quantity of rights; 0 - equate to the quantity of rights; it will be deducted 1 if rights and lefts match when 1 < c = quantity of rights)')
        opt_grp.add_argument('-s', '--srt-field-group', metavar='[None]', dest='srt_field_group', type=str,
                             help='Dot paths to sorted fields (plus separated without spaces; src-db-VCF: [[#CHROM+POS]]; src-db-BED: [[chrom+start+end]]; src-db-VCF, src-db-BED: trg-TSV)')
        opt_grp.add_argument('-o', '--srt-order', metavar='[asc]', choices=['asc', 'desc'], default='asc', dest='srt_order', type=str,
                             help='{asc, desc} Order of sorting (applicable with -s)')
        opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                             help='Selected top level fields (comma separated without spaces; src-db-VCF, src-db-BED: trg-TSV; _id field will not be output)')
        opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Punctuation mark to restore a cell from a list (src-db-VCF, src-db-BED (trg-BED): not applicable)')
        args = arg_parser.parse_args()
        return args
