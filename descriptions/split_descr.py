__version__ = 'v1.1'


class SplitDescr():
    def __init__(self, version, authors):
        self.ru = f'''
Программа, позволяющая разбить каждую
коллекцию MongoDB-базы по заданному полю.

Версия: {version}
Требуемые сторонние компоненты: MongoDB, PyMongo
Авторы: {chr(10).join(authors)}
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Разделяемые данные должны находиться в базе, созданной
с помощью create или других инструментов high-perf-bio.

Чтобы программа работала быстро, нужен индекс
поля, по которому осуществляется деление.

К вложенному полю обращайтесь через точку:
"field_1.field_2.(...).field_N"

Условные обозначения в справке по CLI и частично GUI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
scr/trg-db-FMT - коллекции исходной/конечной БД,
соответствующие по структуре таблицам определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - сортируемые поля, а также поля,
для которых создавать составной индекс.
'''
        self.en = f'''
A program that allows to split each collection
of the MongoDB database by specified field.

Version: {version}
Dependencies: MongoDB, PyMongo
Authors: {chr(10).join(authors)}
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

The divided data must be in the DB produced
by "create" or other high-perf-bio tools.

For the program to process quickly, there needs
index of the field by which the splitting is done.

Call the nested field using a point:
"field_1.field_2.(...).field_N"

Notation in the CLI help and partially in the GUI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}};
scr/trg-db-FMT - collections of source/target DB,
matching by structure to the tables in a certain format;
trg-FMT - target tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error;
f1+f2+f3 - sorted fields, as well as fields,
for which to create a compound index.
'''
