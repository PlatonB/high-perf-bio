__version__ = 'v1.2'


class DockDescr():
    def __init__(self, version, authors):
        self.ru = f'''
Программа, получающая характеристики элементов
выбранного табличного столбца по MongoDB-базе
с сохранением исходных характеристик.

Версия: {version}
Требуемые сторонние компоненты: MongoDB, PyMongo
Авторы: {chr(10).join(authors)}
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
помощью create или других инструментов high-perf-bio.

Чтобы программа работала быстро, нужны индексы вовлечённых в запрос полей.

Условные обозначения в справке по CLI и частично GUI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-FMT - аннотируемые таблицы определённого формата;
src-db-FMT - коллекции исходной БД, соответствующие
по структуре таблицам определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку.
'''
        self.en = f'''
A program that retrieves the characteristics of
elements from chosen table's column by MongoDB
database with keeping the original characteristics.

Version: {version}
Dependencies: MongoDB, PyMongo
Authors: {chr(10).join(authors)}
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
BED        |  Not required  |  Do plus 1
TSV        |  Required      |  Do not plus 1

Annotated column must be in the same position in all source tables.

Also as an experiment there is the possibility of intersection
by coordinates. All 4 combinations of VCF and BED are supported.

Each annotated table must be compressed using GZIP.

The source of the characteristics must be the DB
produced by "create" or other high-perf-bio tools.

For the program to work fast, it needs
indexes of the fields involved in the query.

Notation in the CLI help and partially in the GUI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}};
src-FMT - annotated tables in a certain format;
scr-db-FMT - collections of source DB, matching
by structure to the tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error.
'''
