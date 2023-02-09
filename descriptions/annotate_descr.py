__version__ = 'v1.1'

class AnnotateDescr():
        def __init__(self, version, authors):
                self.ru=f'''
Программа, получающая характеристики
элементов выбранного столбца по MongoDB-базе.

Версия: {version}
Требуемые сторонние компоненты: MongoDB, PyMongo
Авторы: {chr(10).join(authors)}
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Аннотируемый столбец:
- должен занимать одно и то же положение во всех исходных таблицах;
- не должен быть столь большим, чтобы соответствующий запрос весил более
16 МБ. Крупные аннотируемые данные попробуйте разбить с помощью инструмента split.

Также в качестве эксперимента существует возможность пересечения
по координатам. Поддерживаются все 4 комбинации VCF и BED.

Каждая аннотируемая таблица обязана быть сжатой с помощью GZIP.

Источником характеристик должна быть БД, созданная с
помощью create_db или других инструментов high-perf-bio.

Чтобы программа работала быстро, нужны индексы вовлечённых в запрос полей.

Условные обозначения в справке по CLI и частично GUI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-FMT - аннотируемые таблицы определённого формата;
scr/trg-db-FMT - коллекции исходной/конечной БД,
соответствующие по структуре таблицам определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - сортируемые поля, а также поля,
для которых создавать составной индекс.
'''
                self.en=f'''
A program that retrieves the characteristics of
elements of the chosen column from MongoDB database.

Version: {version}
Dependencies: MongoDB, PyMongo
Authors: {chr(10).join(authors)}
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

Annotated column:
- must be in the same position in all source tables;
- must not be so large that the corresponding query weighs more
than 16 MB. Try to divide large annotated data using the "split" tool.

Also as an experiment there is the possibility of intersection
by coordinates. All 4 combinations of VCF and BED are supported.

Each annotated table must be compressed using GZIP.

The source of the characteristics must be the DB
produced by "create_db" or other high-perf-bio tools.

For the program to work fast, it needs
indexes of the fields involved in the query.

Notation in the CLI help and partially in the GUI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}};
src-FMT - annotated tables in a certain format;
scr/trg-db-FMT - collections of source/target DB,
matching by structure to the tables in a certain format;
trg-FMT - target tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error;
f1+f2+f3 - sorted fields, as well as fields,
for which to create a compound index.
'''
