__version__ = 'v1.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, получающая характеристики элементов
выбранного табличного столбца по MongoDB-базе
с сохранением исходных характеристик.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2022
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

По-сути, это двухсторонний аннотатор, т.е. его можно использовать в
том числе и для аннотации поля из коллекций БД по текстовым файлам.

Возможность протаскивания строк аннотируемых таблиц в конечные файлы
привела к появлению ограничений функциональности по сравнению с annotate:
- не проверяется правильность ввода имён отбираемых полей;
- нет возможности сортировать конечные таблицы;
- не удаляются дубли аннотируемых элементов;
- нельзя выводить результаты в БД;
- конечные таблицы не могут быть в форматах VCF и BED.

Зато нет лимита на размер характеризуемого столбца исходных таблиц.

Требования к наличию табличной шапки (набора имён столбцов)
и подсчёту количества нечитаемых программой строк:

Формат     |  Наличие         |  Модификация значения
src-файла  |  шапки           |  -m при наличии шапки
-----------------------------------------------------
VCF        |  Обязательно     |  Аргумент не применим
BED        |  Не обязательно  |  Прибавьте 1
TSV        |  Обязательно     |  Не прибавляйте 1

Аннотируемый столбец должен занимать одно
и то же положение во всех исходных таблицах.

Также в качестве эксперимента существует возможность пересечения
по координатам. Поддерживаются все 4 комбинации VCF и BED.

Каждая аннотируемая таблица обязана быть сжатой с помощью GZIP.

Источником характеристик должна быть БД, созданная с
помощью create_db или других инструментов high-perf-bio.

Чтобы программа работала быстро, нужны индексы вовлечённых в запрос полей.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-FMT - аннотируемые таблицы определённого формата;
src-db-FMT - коллекции исходной БД, соответствующие
по структуре таблицам определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку.
''',
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Путь к папке со сжатыми аннотируемыми таблицами')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Имя БД, по которой аннотировать')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Путь к папке для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно аннотируемых таблиц')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Количество строк метаинформации аннотируемых таблиц (src-VCF: не применяется; src-BED: включите шапку (если есть); src-TSV: не включайте шапку)')
        opt_grp.add_argument('-n', '--by-loc', dest='by_loc', action='store_true',
                             help='Пересекать по геномной локации (экспериментальная фича; src-TSV, src-db-TSV: не применяется)')
        opt_grp.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                             help='Номер аннотируемого столбца (применяется без -n; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
        opt_grp.add_argument('-f', '--ann-field-path', metavar='[None]', dest='ann_field_path', type=str,
                             help='Точечный путь к полю, по которому аннотировать (применяется без -n; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[первое после _id поле]])')
        opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                             help='Отбираемые поля верхнего уровня и/или столбцы (через запятую без пробела; имя_столбца_f; поле _id не выведется)')
        opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Знак препинания для восстановления ячейки из списка')
        args = arg_parser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that retrieves the characteristics of
elements from chosen table's column by MongoDB
database with keeping the original characteristics.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2022
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

In essence, it is a bidirectional annotator, i.e. it can be used
in addition for annotating fields from DB collections by text files.

The ability to drag the rows of annotated tables into the target
files led to functionality limitations compared to "annotate":
- there is no check for the correctness of typed names of the selected fields;
- it is not possible to sort the target tables;
- does not deleted duplicates of annotated items;
- cannot output results to DB;
- target tables cannot be in VCF and BED formats.

But there is no limit on the size of
the source tables' characterized column.

Requirements for table header (set of column names) and
for counting the number of rows unreadable by the program:

Format of  |  Presence of   |  Modification of -m value
src file   |  header        |  if header is present
---------------------------------------------------------
VCF        |  Required      |  Argument is not applicable
BED        |  Not required  |  Plus 1
TSV        |  Required      |  Do not plus 1

Annotated column must be in the same position in all source tables.

Also as an experiment there is the possibility of intersection
by coordinates. All 4 combinations of VCF and BED are supported.

Each annotated table must be compressed using GZIP.

The source of the characteristics must be the DB
produced by "create_db" or other high-perf-bio tools.

For the program to work fast, it needs
indexes of the fields involved in the query.

The notation in the CLI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}};
src-FMT - annotated tables in a certain format;
scr-db-FMT - collections of source DB, matching
by structure to the tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error.
''',
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Path to directory with gzipped tables, which will be annotated')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Name of the DB by which to annotate')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Path to directory for results')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum quantity of parallel annotated tables')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Quantity of metainformation lines of annotated tables (src-VCF: not applicable; src-BED: include a header (if available); src-TSV: do not include a header)')
        opt_grp.add_argument('-n', '--by-loc', dest='by_loc', action='store_true',
                             help='Intersect by genomic location (experimental feature; src-TSV, src-db-TSV: not applicable)')
        opt_grp.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                             help='Number of the annotated column (applied without -n; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
        opt_grp.add_argument('-f', '--ann-field-path', metavar='[None]', dest='ann_field_path', type=str,
                             help='Dot path to the field by which to annotate (applied without -n; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[first field after _id]])')
        opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                             help='Selected top level fields and/or columns (comma separated without spaces; column_name_f; _id field will not be output)')
        opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Punctuation mark to restore a cell from a list')
        args = arg_parser.parse_args()
        return args
