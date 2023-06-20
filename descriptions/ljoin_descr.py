__version__ = 'v1.2'


class LjoinDescr():
    def __init__(self, version, authors):
        self.ru = f'''
Программа, выполняющая пересечение
или вычитание коллекций по выбранному
полю или по геномным координатам.

Версия: {version}
Требуемые сторонние компоненты: MongoDB, PyMongo
Авторы: {chr(10).join(authors)}
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

Условные обозначения в справке по CLI и частично GUI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-db-FMT - коллекции исходной БД, соответствующие
по структуре таблицам определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - сортируемые поля.
'''
        self.en = f'''
A program that performs intersection or subtraction of
collections by chosen field or by genomic coordinates.

Version: {version}
Dependencies: MongoDB, PyMongo
Authors: {chr(10).join(authors)}
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

Notation in the CLI help and partially in the GUI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}};
scr-db-FMT - collections of source DB, matching
by structure to the tables in a certain format;
trg-FMT - target tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error.
f1+f2+f3 - sorted fields.
'''
